// src/aggregate/aggregate.service.ts
import { PgClient } from '../pg/pg.client';
import { SuiRpcClient } from '../sui/sui.client';
import { HistoricalPriceGetter } from '../redis/redis.client';
import * as fs from 'fs';
import * as path from 'path';

export interface PoolTypeEntry {
    pool: string;
    typeA: string;
    typeB: string;
}

export interface AggregateResult {
    poolTypeList: PoolTypeEntry[];
    startTs: number;
    endTs: number;
}

export interface AggregateService {
    /**
     * Run the daily aggregation job.
     * Returns collected pool types and the locked time window.
     */
    runDailyAggregationJob: () => Promise<AggregateResult>;
}

function sleep(ms: number) {
    return new Promise<void>(res => setTimeout(res, ms));
}

function parsePoolTypes(typeString: string): [string, string] {
    const match = typeString.match(/<([^,]+),\s*([^>]+)>/);
    if (match) return [match[1].trim(), match[2].trim()];
    return ['', ''];
}

function alignToMinute(ts: number): number {
    return ts - (ts % 60);
}

/**
 * createAggregateService
 *
 * - dbClient: PgClient (has .query(sql, params))
 * - suiClient: SuiRpcClient (has .getObject({id, options}))
 * - priceGetter: HistoricalPriceGetter (your redis client)
 */
export function createAggregateService(dbClient: PgClient, suiClient: SuiRpcClient, priceGetter: HistoricalPriceGetter): AggregateService {
    const AGG_PATH = path.join(__dirname, '..', '..', 'sql', 'aggregate.sql');

    function loadAggregateSql(): { sql: string; hasParams: boolean } {
        try {
            const text = fs.readFileSync(AGG_PATH, 'utf8');
            const hasParams = /\$1|\$2/.test(text);
            return { sql: text, hasParams };
        } catch (e) {
            return { sql: '', hasParams: false };
        }
    }

    /**
     * Given tokenType and swap timestamp (seconds), query two minute-aligned prices and pick nearest.
     * Returns number or null.
     */
    async function getNearestPrice(tokenType: string, swapTs: number): Promise<number | null> {
        const t0 = alignToMinute(swapTs);
        const t1 = t0 + 60;

        try {
            // Note: using two parallel requests is simplest. In production consider pipelining many keys.
            const [v0Raw, v1Raw] = await Promise.all([
                priceGetter.getHistoricalPrice(tokenType, t0),
                priceGetter.getHistoricalPrice(tokenType, t1)
            ]);

            const v0 = v0Raw != null ? parseFloat(v0Raw) : null;
            const v1 = v1Raw != null ? parseFloat(v1Raw) : null;

            if (v0 == null && v1 == null) return null;
            if (v0 != null && v1 == null) return v0;
            if (v0 == null && v1 != null) return v1;

            // both exist -> choose closer
            const d0 = Math.abs(swapTs - t0);
            const d1 = Math.abs(t1 - swapTs);
            return d0 <= d1 ? v0! : v1!;
        } catch (err) {
            console.error('[Aggregate Service] getNearestPrice error:', (err as Error).message);
            return null;
        }
    }

    /**
     * Compute usdValue for a swap based on rule:
     * - try inputPrice * amountIn
     * - else try outputPrice * amountOut
     */
    async function computeUsdValueForSwap(
        aToB: boolean,
        poolTypes: { typeA: string; typeB: string },
        amountAIn: number,
        amountBIn: number,
        amountAOut: number,
        amountBOut: number,
        ts: number
    ): Promise<number> {
        const inputType = aToB ? poolTypes.typeA : poolTypes.typeB;
        const outputType = aToB ? poolTypes.typeB : poolTypes.typeA;

        const amountIn = aToB ? amountAIn : amountBIn;
        const amountOut = aToB ? amountBOut : amountAOut;

        const inputPrice = await getNearestPrice(inputType, ts);
        if (inputPrice != null && !Number.isNaN(amountIn)) {
            return inputPrice * amountIn;
        }

        const outputPrice = await getNearestPrice(outputType, ts);
        if (outputPrice != null && !Number.isNaN(amountOut)) {
            return outputPrice * amountOut;
        }

        return 0;
    }

    return {
        async runDailyAggregationJob(): Promise<AggregateResult> {
            console.log('\n--- [Aggregate Service] START runDailyAggregationJob ---');

            // ---------------------
            // 1) lock time window
            // ---------------------
            const nowSec = Math.floor(Date.now() / 1000);
            const endTs = nowSec - 1;                // ensure < now
            const startTs = endTs - 24 * 3600;
            console.log(`[Aggregate Service] locked window: ${startTs} -> ${endTs}`);

            // ---------------------
            // 2) find all pools in window
            // ----------------------
            const poolTypeList: PoolTypeEntry[] = [];
            try {
                const GET_POOLS_SQL = `
            SELECT DISTINCT pool
            FROM public.cetus_swap
            WHERE "timestamp" >= $1 AND "timestamp" <= $2
        `;
                const poolsRes = await dbClient.query(GET_POOLS_SQL, [startTs, endTs]);
                const pools: string[] = poolsRes.rows.map((r: any) => r.pool);
                console.log(`[Aggregate Service] ${pools.length} distinct pools found in window.`);

                // ---------------------
                // 3) get object types for pools
                // ---------------------
                for (const poolId of pools) {
                    try {
                        const resp = await suiClient.getObject({
                            id: poolId,
                            options: { showType: true, showContent: false },
                        });

                        if (!resp || resp.error || !resp.data || !resp.data.type) {
                            console.warn(`[Aggregate Service] skip pool ${poolId}: invalid object/type`);
                        } else {
                            const objectType = resp.data.type as string;
                            const [typeA, typeB] = parsePoolTypes(objectType);
                            poolTypeList.push({ pool: poolId, typeA, typeB });
                        }
                    } catch (e) {
                        console.error(`[Aggregate Service] error fetching pool ${poolId}:`, (e as Error).message);
                    }

                    await sleep(200); // 200ms interval
                }

                console.log(`[Aggregate Service] collected types for ${poolTypeList.length} pools.`);

                const poolTypeMap = new Map<string, { typeA: string; typeB: string }>();
                for (const e of poolTypeList) poolTypeMap.set(e.pool, { typeA: e.typeA, typeB: e.typeB });

                // ===================================================
                // 4) BATCH PROCESS — UPDATED AGGREGATION LOGIC
                // ===================================================

                const batchSize = 1000;
                let offset = 0;
                let batchIndex = 0;
                let isFirstBatch = true;

                const summaryDate = new Date(endTs * 1000).toISOString().slice(0, 10);

                while (true) {
                    const FETCH_SWAPS_SQL = `
                SELECT
                    pool,
                    a_to_b,
                    amount_a_in, amount_b_in,
                    amount_a_out, amount_b_out,
                    fee_amount_a, fee_amount_b,
                    "timestamp"
                FROM public.cetus_swap
                WHERE "timestamp" >= $1 AND "timestamp" <= $2
                ORDER BY "timestamp" ASC
                LIMIT $3 OFFSET $4
            `;

                    const swapsRes = await dbClient.query(FETCH_SWAPS_SQL, [
                        startTs, endTs, batchSize, offset
                    ]);
                    const swaps = swapsRes.rows;

                    if (!swaps || swaps.length === 0) {
                        console.log('[Aggregate Service] no more swaps to process.');
                        break;
                    }

                    batchIndex++;
                    console.log(`[Aggregate Service] processing batch ${batchIndex}, ${swaps.length} swaps (offset ${offset}).`);

                    // ------------------------------
                    // per-pool batch aggregation
                    // ------------------------------
                    const batchAgg = new Map<string, {
                        totalIn: number;
                        totalOut: number;
                        totalUsd: number;
                        swapCount: number;
                        totalFeeA: number;
                        totalFeeB: number;
                    }>();

                    for (const row of swaps) {
                        const poolId = row.pool;
                        const poolInfo = poolTypeMap.get(poolId);
                        if (!poolInfo) continue;

                        const aToB = !!row.a_to_b;
                        const amountAIn = Number(row.amount_a_in) || 0;
                        const amountBIn = Number(row.amount_b_in) || 0;
                        const amountAOut = Number(row.amount_a_out) || 0;
                        const amountBOut = Number(row.amount_b_out) || 0;
                        const feeA = Number(row.fee_amount_a) || 0;
                        const feeB = Number(row.fee_amount_b) || 0;
                        const ts = Number(row.timestamp);

                        const usdValue = await computeUsdValueForSwap(
                            aToB, poolInfo,
                            amountAIn, amountBIn,
                            amountAOut, amountBOut,
                            ts
                        );

                        const amountIn = aToB ? amountAIn : amountBIn;
                        const amountOut = aToB ? amountBOut : amountAOut;

                        const prev =
                            batchAgg.get(poolId) ||
                            { totalIn: 0, totalOut: 0, totalUsd: 0, swapCount: 0, totalFeeA: 0, totalFeeB: 0 };

                        prev.totalIn += amountIn;
                        prev.totalOut += amountOut;
                        prev.totalUsd += usdValue;
                        prev.swapCount += 1;
                        prev.totalFeeA += feeA;
                        prev.totalFeeB += feeB;

                        batchAgg.set(poolId, prev);
                    }

                    // ------------------------------
                    // UPSERT summary table
                    // ------------------------------
                    for (const [poolId, v] of batchAgg.entries()) {
                        if (isFirstBatch) {
                            const UPSERT_REPLACE_SQL = `
                        INSERT INTO public.cetus_swap_daily_summary
                            (pool, date,
                             total_amount_in,
                             total_amount_out,
                             total_usd,
                             swap_count,
                             total_fee_a,
                             total_fee_b)
                        VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
                        ON CONFLICT (pool, date) DO UPDATE SET
                            total_amount_in  = EXCLUDED.total_amount_in,
                            total_amount_out = EXCLUDED.total_amount_out,
                            total_usd        = EXCLUDED.total_usd,
                            swap_count       = EXCLUDED.swap_count,
                            total_fee_a      = EXCLUDED.total_fee_a,
                            total_fee_b      = EXCLUDED.total_fee_b
                    `;
                            await dbClient.query(UPSERT_REPLACE_SQL, [
                                poolId, summaryDate,
                                v.totalIn, v.totalOut, v.totalUsd,
                                v.swapCount, v.totalFeeA, v.totalFeeB
                            ]);
                        } else {
                            const UPSERT_ADD_SQL = `
                        INSERT INTO public.cetus_swap_daily_summary
                            (pool, date,
                             total_amount_in,
                             total_amount_out,
                             total_usd,
                             swap_count,
                             total_fee_a,
                             total_fee_b)
                        VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
                        ON CONFLICT (pool, date) DO UPDATE SET
                            total_amount_in  = public.cetus_swap_daily_summary.total_amount_in + EXCLUDED.total_amount_in,
                            total_amount_out = public.cetus_swap_daily_summary.total_amount_out + EXCLUDED.total_amount_out,
                            total_usd        = public.cetus_swap_daily_summary.total_usd + EXCLUDED.total_usd,
                            swap_count       = public.cetus_swap_daily_summary.swap_count + EXCLUDED.swap_count,
                            total_fee_a      = public.cetus_swap_daily_summary.total_fee_a + EXCLUDED.total_fee_a,
                            total_fee_b      = public.cetus_swap_daily_summary.total_fee_b + EXCLUDED.total_fee_b
                    `;
                            await dbClient.query(UPSERT_ADD_SQL, [
                                poolId, summaryDate,
                                v.totalIn, v.totalOut, v.totalUsd,
                                v.swapCount, v.totalFeeA, v.totalFeeB
                            ]);
                        }
                    }

                    if (isFirstBatch) isFirstBatch = false;

                    offset += swaps.length;
                    console.log(`[Aggregate Service] batch ${batchIndex} finished (${batchAgg.size} pools).`);
                }

                console.log('--- [Aggregate Service] FINISHED ---');
                return { poolTypeList, startTs, endTs };
            } catch (err) {
                console.error('[Aggregate Service] CRITICAL ERROR:', (err as Error).message);
                return { poolTypeList, startTs, endTs };
            }
        },
    };
}
