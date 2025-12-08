// src/services/liquidity.service.ts
import { PgClient } from '../pg/pg.client';
import { SuiRpcClient } from '../sui/sui.client';
import { LiquiditySnapshotRecord } from '../types/db.types';

const GET_POOLS_SQL = `SELECT DISTINCT pool FROM public.cetus_swap`;

function parsePoolTypes(typeString: string): [string, string] {
    const match = typeString.match(/<([^,]+),\s*([^>]+)>/);
    if (match) {
        return [match[1].trim(), match[2].trim()];
    }
    return ['', ''];
}

export interface LiquidityService {
    runLiquiditySnapshot: () => Promise<void>;
}

export function createLiquidityService(dbClient: PgClient, suiClient: SuiRpcClient): LiquidityService {
    
    async function saveSnapshot(record: LiquiditySnapshotRecord) {
        const sql = `
            INSERT INTO public.cetus_liquidity_snapshot (pool, amount_a, amount_b, type_a, type_b, "timestamp")
            VALUES ($1, $2, $3, $4, $5, $6)
        `;
        try {
            await dbClient.query(sql, [
                record.pool, record.amount_a, record.amount_b, 
                record.type_a, record.type_b, record.timestamp
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
                const poolQueryResult = await dbClient.query(GET_POOLS_SQL);
                const poolIds = poolQueryResult.rows.map(row => row.pool);
                console.log(`Found ${poolIds.length} unique pools to process.`);

                if (poolIds.length === 0) return;

                const objectResponses = await suiClient.multiGetObjects({
                    ids: poolIds,
                    options: { 
                        showContent: true,
                        showType: true,
                    },
                });

                for (const response of objectResponses) {
                    if (response.error || !response.data || !response.data.content) {
                        console.warn(`Skipping pool (Error/No Content): ${response.error?.code || response.data?.objectId}`);
                        continue;
                    }
                    
                    const fields = (response.data.content as any).fields;
                    const objectType = response.data.type || '';

                    if (!fields || !objectType.includes('::pool::Pool<')) {
                         console.warn(`Skipping object ${response.data.objectId}: Not a recognized Pool object.`);
                         continue;
                    }

                    const [type_a, type_b] = parsePoolTypes(objectType);
                    
                    const record: LiquiditySnapshotRecord = {
                        pool: response.data.objectId,
                        amount_a: parseFloat(fields.coin_a), 
                        amount_b: parseFloat(fields.coin_b),
                        type_a: type_a,
                        type_b: type_b,
                        timestamp: currentTimestamp,
                    };

                    await saveSnapshot(record);
                }

                const duration = Date.now() - startTime;
                console.log(`Job complete. Duration: ${duration}ms.`);

            } catch (error) {
                console.error("CRITICAL: Snapshot job failed.", (error as Error).message);
            }
        },
    };
}