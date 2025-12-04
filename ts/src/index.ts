import * as dotenv from 'dotenv';
import { createPgClient } from './pg/pg.client';
import { createSuiClient } from './sui/sui.client';
import { createAggregateService, AggregateService } from './services/aggregate';
import { createLiquidityService, LiquidityService } from './services/liquidity';
import { createPruneService, PruneService } from './services/prune';
import { startScheduler } from './scheduler';

// 加载环境变量

async function bootstrap() {
    console.log("--- Worker Application Startup ---");
    
    const pgClientInstance = createPgClient(process.env.DATABASE_URL || '');
    const suiClientInstance = createSuiClient(process.env.SUI_RPC_URL || '');
    
    try {
        await pgClientInstance.connect();
    } catch (error) {
        console.error("🛑 FATAL ERROR: Failed to establish PostgreSQL connection.");
        throw error;
    }

    const aggregateService: AggregateService = createAggregateService(pgClientInstance);
    const liquidityService: LiquidityService = createLiquidityService(pgClientInstance, suiClientInstance);
    const pruneService: PruneService = createPruneService(pgClientInstance);
    
    startScheduler(aggregateService, liquidityService, pruneService, suiClientInstance); // 👈 传入 PruneService 

    console.log("--- Worker Application Running in background ---");
}

bootstrap().catch(err => {
    console.error("🛑 FATAL ERROR: Application failed to start.", err);
    process.exit(1); 
});