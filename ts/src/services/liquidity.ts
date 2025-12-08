// src/services/liquidity.service.ts
import { PgClient } from '../pg/pg.client';
import { SuiRpcClient } from '../sui/sui.client';
import { HistoricalPriceGetter } from '../redis/redis.client';
import { LiquiditySnapshotRecord } from '../types/db.types';

const GET_POOLS_SQL = `SELECT DISTINCT pool FROM public.cetus_swap`;

function parsePoolTypes(typeString: string): [string, string] {
    const match = typeString.match(/<([^,]+),\s*([^>]+)>/);
    if (match) return [match[1].trim(), match[2].trim()];
    return ['', ''];
}

export interface LiquidityService {
    runLiquiditySnapshot: () => Promise<void>;
}

export function createLiquidityService(
    dbClient: PgClient,
    suiClient: SuiRpcClient,
    priceGetter: HistoricalPriceGetter
): LiquidityService {

    async function getNearestPrice(tokenType: string, ts: number): Promise<number | null> {
        const t0 = ts - (ts % 60);
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

            const d0 = Math.abs(ts - t0);
            const d1 = Math.abs(t1 - ts);
            return d0 <= d1 ? v0! : v1!;
        } catch {
            return null;
        }
    }

    async function saveSnapshot(record: LiquiditySnapshotRecord & { tvl: number }) {
        const sql = `
            INSERT INTO public.cetus_liquidity_snapshot 
                (pool, amount_a, amount_b, type_a, type_b, "timestamp", tvl)
            VALUES ($1, $2, $3, $4, $5, $6, $7)
        `;
        try {
            await dbClient.query(sql, [
                record.pool,
                record.amount_a,
                record.amount_b,
                record.type_a,
                record.type_b,
                record.timestamp,
                record.tvl
            ]);
        } catch (e) {
            console.error(`Error saving snapshot for pool ${record.pool}:`, (e as Error).message);
        }
    }

    return {
        async runLiquiditySnapshot() {
            const startTime = Date.now();
            const currentTimestamp = Math.floor(startTime / 1000);

            try {
                const poolQuery = await dbClient.query(GET_POOLS_SQL);
                const poolIds = poolQuery.rows.map(r => r.pool);
                console.log(`Found ${poolIds.length} unique pools to process.`);

                if (poolIds.length === 0) return;

                const BATCH_LIMIT = 50; // Sui RPC multiGetObjects 限制
                for (let i = 0; i < poolIds.length; i += BATCH_LIMIT) {
                    const batchIds = poolIds.slice(i, i + BATCH_LIMIT);

                    const objectResponses = await suiClient.multiGetObjects({
                        ids: batchIds,
                        options: { showContent: true, showType: true }
                    });

                    for (const response of objectResponses) {
                        try {
                            if (response.error || !response.data || !response.data.content) {
                                console.warn(`Skipping pool (Error/No Content): ${response.error?.code || response.data?.objectId}`);
                                continue;
                            }

                            const fields = (response.data.content as any).fields;
                            const objectType = response.data.type || '';
                            if (!fields || !objectType.includes('::pool::Pool<')) {
                                console.warn(`Skipping object ${response.data.objectId}: Not a Pool object.`);
                                continue;
                            }

                            const [typeA, typeB] = parsePoolTypes(objectType);
                            const amountA = parseFloat(fields.coin_a);
                            const amountB = parseFloat(fields.coin_b);

                            const priceA = await getNearestPrice(typeA, currentTimestamp);
                            const priceB = await getNearestPrice(typeB, currentTimestamp);

                            const decimalsA = priceGetter.getCoinDecimals
                                ? await priceGetter.getCoinDecimals(typeA)
                                : 0;
                            const decimalsB = priceGetter.getCoinDecimals
                                ? await priceGetter.getCoinDecimals(typeB)
                                : 0;

                            const tvl =
                                (priceA ? (amountA * priceA) / Math.pow(10, decimalsA!) : 0) +
                                (priceB ? (amountB * priceB) / Math.pow(10, decimalsB!) : 0);

                            const record = {
                                pool: response.data.objectId,
                                amount_a: amountA,
                                amount_b: amountB,
                                type_a: typeA,
                                type_b: typeB,
                                timestamp: currentTimestamp,
                                tvl
                            };

                            await saveSnapshot(record);
                        } catch (e) {
                            //
                        }
                    }
                }

                const duration = Date.now() - startTime;
                console.log(`Liquidity snapshot complete. Duration: ${duration}ms.`);
            } catch (err) {
                console.error(`CRITICAL: Snapshot job failed.`, (err as Error).message);
            }
        }
    };
}
