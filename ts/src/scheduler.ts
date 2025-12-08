import { AggregateService } from './services/aggregate';
import { LiquidityService } from './services/liquidity';
import { PruneService } from './services/prune';
import { SuiRpcClient } from './sui/sui.client';

const ONE_DAY_MS = 24 * 60 * 60 * 1000;       // 24 小时
const FOUR_HOURS_MS = 4 * 60 * 60 * 1000;    // 4 小时

/**
 * 启动任务调度器。
 * @param aggregateService - 注入的费用聚合服务实例 (24小时任务)
 * @param liquidityService - 注入的流动性快照服务实例 (4小时任务)
 * @param pruneService - 注入的 Sui Client 实例 (用于基础设施连接检查)
 * @param suiClient - 注入的 Sui Client 实例 (用于基础设施连接检查)
 */
export function startScheduler(
    aggregateService: AggregateService,
    liquidityService: LiquidityService,
    pruneService: PruneService,
    suiClient: SuiRpcClient
) {
    console.log("\n--- Scheduler Initializing ---");

    console.log(`[Scheduler] Scheduling Daily Aggregation Job to run every 24 hours.`);
    aggregateService.runDailyAggregationJob().catch(e => console.error("Initial Daily Aggregation Failed:", e));
    setInterval(() => {
        aggregateService.runDailyAggregationJob().catch(e => console.error("Daily Aggregation Failed:", e));
    }, ONE_DAY_MS);

    console.log(`[Scheduler] Scheduling Liquidity Snapshot Job to run every 4 hours.`);
    liquidityService.runLiquiditySnapshot().catch(e => console.error("Initial Liquidity Snapshot Failed:", e));
    setInterval(() => {
        liquidityService.runLiquiditySnapshot().catch(e => console.error("Liquidity Snapshot Failed:", e));
    }, FOUR_HOURS_MS);

    console.log(`[Scheduler] Scheduling Daily Pruning Job.`);
    pruneService.runOldDataPruning().catch(e => console.error("Initial Pruning Failed:", e));
    setInterval(() => {
        pruneService.runOldDataPruning().catch(e => console.error("Pruning Failed:", e));
    }, ONE_DAY_MS);

    console.log(`[Scheduler] Scheduling Sui Heartbeat check every 5 minutes.`);
    setInterval(() => {
        suiClient.getChainIdentifier().catch(() => console.error("[Heartbeat] Sui RPC connection lost."));
    }, 5 * 60 * 1000);

    console.log("--- Scheduler Running. All tasks are active. ---\n");
}