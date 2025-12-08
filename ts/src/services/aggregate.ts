// src/aggregate/aggregate.service.ts
import { PgClient } from '../pg/pg.client';
import { SuiRpcClient } from '../sui/sui.client';
import { HistoricalPriceGetter } from '../redis/redis.client';

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

export function createAggregateService(
    dbClient: PgClient,
    suiClient: SuiRpcClient,
    priceGetter: HistoricalPriceGetter
): AggregateService {

    async function getNearestPrice(tokenType: string, swapTs: number): Promise<number | null> {
        const t0 = alignToMinute(swapTs);
        const t1 = t0 + 60;
        try {
            const [v0Raw, v1Raw] = await Promise.all([
                priceGetter.getHistoricalPrice(tokenType, t0),
                priceGetter.getHistoricalPrice(tokenType, t1)
            ]);
            const v0 = v0Raw != null ? parseFloat(v0Raw) : null;
            const v1 = v1Raw != null ? parseFloat(v1Raw) : null;
            if (v0 == null && v1 == null) return null;
            if (v0 != null && v1 == null) return v0;
            if (v0 == null && v1 != null) return v1;
            return Math.abs(swapTs - t0) <= Math.abs(t1 - swapTs) ? v0! : v1!;
        } catch {
            return null;
        }
    }

    async function computeUsdValueForSwap(
        poolTypes: { typeA: string; typeB: string },
        amountAIn: number,
        amountBIn: number,
        amountAOut: number,
        amountBOut: number,
        ts: number
    ): Promise<number> {
        const priceA = await getNearestPrice(poolTypes.typeA, ts);
        const priceB = await getNearestPrice(poolTypes.typeB, ts);
        const inValue = (priceA ? priceA * amountAIn : 0) + (priceB ? priceB * amountBIn : 0);
        const outValue = (priceA ? priceA * amountAOut : 0) + (priceB ? priceB * amountBOut : 0);
        return inValue > 0 ? inValue : outValue;
    }

    return {
        async runDailyAggregationJob(): Promise<AggregateResult> {
            const nowSec = Math.floor(Date.now() / 1000);
            const endTs = nowSec - 1;
            const startTs = endTs - 24 * 3600;

            const poolTypeList: PoolTypeEntry[] = [];
            try {
                const GET_POOLS_SQL = `
                    SELECT DISTINCT pool
                    FROM public.cetus_swap
                    WHERE "timestamp" >= $1 AND "timestamp" <= $2
                `;
                const poolsRes = await dbClient.query(GET_POOLS_SQL, [startTs, endTs]);
                const pools: string[] = poolsRes.rows.map((r: any) => r.pool);

                for (const poolId of pools) {
                    try {
                        const resp = await suiClient.getObject({
                            id: poolId,
                            options: { showType: true, showContent: false },
                        });
                        if (!resp || resp.error || !resp.data || !resp.data.type) continue;
                        const [typeA, typeB] = parsePoolTypes(resp.data.type as string);
                        poolTypeList.push({ pool: poolId, typeA, typeB });
                    } catch { }
                    await sleep(200);
                }

                const poolTypeMap = new Map<string, { typeA: string; typeB: string }>();
                for (const e of poolTypeList) poolTypeMap.set(e.pool, { typeA: e.typeA, typeB: e.typeB });

                const batchSize = 1000;
                let offset = 0;
                let isFirstBatch = true;
                const summaryDate = new Date(endTs * 1000).toISOString().slice(0, 10);

                while (true) {
                    const FETCH_SWAPS_SQL = `
                        SELECT pool, amount_a_in, amount_b_in, amount_a_out, amount_b_out,
                               fee_amount_a, fee_amount_b, "timestamp"
                        FROM public.cetus_swap
                        WHERE "timestamp" >= $1 AND "timestamp" <= $2
                        ORDER BY "timestamp" ASC
                        LIMIT $3 OFFSET $4
                    `;
                    const swapsRes = await dbClient.query(FETCH_SWAPS_SQL, [startTs, endTs, batchSize, offset]);
                    const swaps = swapsRes.rows;
                    if (!swaps || swaps.length === 0) break;

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
                        const amountAIn = Number(row.amount_a_in) || 0;
                        const amountBIn = Number(row.amount_b_in) || 0;
                        const amountAOut = Number(row.amount_a_out) || 0;
                        const amountBOut = Number(row.amount_b_out) || 0;
                        const feeA = Number(row.fee_amount_a) || 0;
                        const feeB = Number(row.fee_amount_b) || 0;
                        const ts = Number(row.timestamp);

                        const usdValue = await computeUsdValueForSwap(poolInfo, amountAIn, amountBIn, amountAOut, amountBOut, ts);
                        const totalIn = amountAIn + amountBIn;
                        const totalOut = amountAOut + amountBOut;

                        const prev = batchAgg.get(poolId) || { totalIn: 0, totalOut: 0, totalUsd: 0, swapCount: 0, totalFeeA: 0, totalFeeB: 0 };
                        prev.totalIn += totalIn;
                        prev.totalOut += totalOut;
                        prev.totalUsd += usdValue;
                        prev.swapCount += 1;
                        prev.totalFeeA += feeA;
                        prev.totalFeeB += feeB;
                        batchAgg.set(poolId, prev);
                    }

                    for (const [poolId, v] of batchAgg.entries()) {
                        if (isFirstBatch) {
                            const UPSERT_REPLACE_SQL = `
                                INSERT INTO public.cetus_swap_daily_summary
(pool, date,
 total_a_in, total_a_out,
 total_b_in, total_b_out,
 total_usd,
 total_fee_a, total_fee_b)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
ON CONFLICT (pool, date) DO UPDATE SET
    total_a_in  = EXCLUDED.total_a_in,
    total_a_out = EXCLUDED.total_a_out,
    total_b_in  = EXCLUDED.total_b_in,
    total_b_out = EXCLUDED.total_b_out,
    total_usd   = EXCLUDED.total_usd,
    total_fee_a = EXCLUDED.total_fee_a,
    total_fee_b = EXCLUDED.total_fee_b;

                            `;
                            await dbClient.query(UPSERT_REPLACE_SQL, [poolId, summaryDate, v.totalIn, v.totalOut, v.totalUsd, v.swapCount, v.totalFeeA, v.totalFeeB]);
                        } else {
                            const UPSERT_ADD_SQL = `
INSERT INTO public.cetus_swap_daily_summary
(pool, date,
 total_a_in, total_a_out,
 total_b_in, total_b_out,
 total_usd,
 total_fee_a, total_fee_b)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
ON CONFLICT (pool, date) DO UPDATE SET
    total_a_in  = public.cetus_swap_daily_summary.total_a_in + EXCLUDED.total_a_in,
    total_a_out = public.cetus_swap_daily_summary.total_a_out + EXCLUDED.total_a_out,
    total_b_in  = public.cetus_swap_daily_summary.total_b_in + EXCLUDED.total_b_in,
    total_b_out = public.cetus_swap_daily_summary.total_b_out + EXCLUDED.total_b_out,
    total_usd   = public.cetus_swap_daily_summary.total_usd + EXCLUDED.total_usd,
    total_fee_a = public.cetus_swap_daily_summary.total_fee_a + EXCLUDED.total_fee_a,
    total_fee_b = public.cetus_swap_daily_summary.total_fee_b + EXCLUDED.total_fee_b;

                            `;
                            await dbClient.query(UPSERT_ADD_SQL, [poolId, summaryDate, v.totalIn, v.totalOut, v.totalUsd, v.swapCount, v.totalFeeA, v.totalFeeB]);
                        }
                    }

                    if (isFirstBatch) isFirstBatch = false;
                    offset += swaps.length;
                }

                return { poolTypeList, startTs, endTs };
            } catch {
                return { poolTypeList, startTs, endTs };
            }
        }
    };
}
