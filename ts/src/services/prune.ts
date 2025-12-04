// src/services/prune.service.ts
import { PgClient } from '../pg/pg.client';
import { SuiRpcClient } from '../sui/sui.client'; 

// --- SQL 语句 (Hardcoded) ---

// 1. 删除 cetus_liquidity_snapshot 和 cetus_swap 中 timestamp 小于当前时间 - 48 小时的记录
// 48小时 = 172800 秒
const PRUNE_SWAP_AND_SNAPSHOTS_SQL = `
    DELETE FROM public.cetus_swap WHERE "timestamp" < (EXTRACT(EPOCH FROM NOW()) - 172800);
    DELETE FROM public.pool_liquidity_snapshot WHERE "timestamp" < (EXTRACT(EPOCH FROM NOW()) - 172800);
`;

// 2. 删除 cetus_swap_daily_summary 中 date 为 2 天之前的记录
const PRUNE_DAILY_SUMMARY_SQL = `
    DELETE FROM public.cetus_swap_daily_summary WHERE date < CURRENT_DATE - INTERVAL '2 days';
`;


// --- Service Factory ---

// 修正接口名称为 PruneService
export interface PruneService {
    runOldDataPruning: () => Promise<void>;
}

export function createPruneService(dbClient: PgClient): PruneService {
    
    return {
        async runOldDataPruning() {
            console.log(`\n--- [Prune Service] START Data Pruning Job ---`);
            const startTime = Date.now();
            
            try {
                // 1. 清理 Swap 和 Snapshot 记录 
                const res1 = await dbClient.query(PRUNE_SWAP_AND_SNAPSHOTS_SQL);
                
                // 2. 清理 Daily Summary 记录 
                const res2 = await dbClient.query(PRUNE_DAILY_SUMMARY_SQL);

                // node-postgres 批量查询返回结果数组
                const swapAndSnapshotRows = res1.rowCount || 0;
                const summaryRows = res2.rowCount || 0;
                
                console.log(`[Prune Service] Pruning SUCCESS.`);
                console.log(`- Pruned ${swapAndSnapshotRows} records from swap/snapshot tables (older than 48h).`);
                console.log(`- Pruned ${summaryRows} records from daily summary table (older than 2 days).`);

            } catch (error) {
                console.error("🛑 [Prune Service] CRITICAL: Database pruning failed.", (error as Error).message);
            }
            
            const duration = Date.now() - startTime;
            console.log(`--- [Prune Service] END Data Pruning Job. Duration: ${duration}ms ---\n`);
        },
    };
}