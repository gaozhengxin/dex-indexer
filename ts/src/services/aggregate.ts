import { PgClient } from '../pg/pg.client'; 
import * as fs from 'fs';
import * as path from 'path';

export interface AggregateService {
    runDailyAggregationJob: () => Promise<void>; 
}

const AGGREGATION_SQL_PATH = path.join(__dirname, '..', '..', 'sql', 'aggregate.sql');
let AGGREGATION_SQL_TASK: string;

try {
    AGGREGATION_SQL_TASK = fs.readFileSync(AGGREGATION_SQL_PATH, 'utf8');
    console.log(`[Aggregate Service] SQL template loaded successfully.`);
} catch (error) {
    console.error(`🛑 [Aggregate Service] ERROR: Failed to read SQL file at ${AGGREGATION_SQL_PATH}`);
    throw error;
}

// --- Service Factory ---
export function createAggregateService(dbClient: PgClient): AggregateService {
    return {
        async runDailyAggregationJob() {
            const oneDayAgoInSeconds = Math.floor(Date.now() / 1000) - (24 * 3600);
            
            console.log(`\n--- [Aggregate Service] START Daily Aggregation Job ---`);
            console.log(`[Aggregate Service] Aggregating swaps since epoch: ${oneDayAgoInSeconds}`);
            const startTime = Date.now();
            
            try {
                const res = await dbClient.query(AGGREGATION_SQL_TASK);
                
                if (res.rowCount !== null && res.rowCount > 0) {
                    console.log(`[Aggregate Service] Aggregation SUCCESS. UPSERTED/INSERTED ${res.rowCount} records into summary table.`);
                } else {
                    console.log(`[Aggregate Service] Aggregation SUCCESS, but 0 records were affected (No recent activity or data was same).`);
                }
                
                const duration = Date.now() - startTime;
                console.log(`--- [Aggregate Service] END Job. Duration: ${duration}ms ---\n`);

            } catch (error) {
                console.error("🛑 [Aggregate Service] CRITICAL: Failed to complete aggregation job.", (error as Error).message);
            }
        },
    };
}