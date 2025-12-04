// src/pg/pg.client.ts
import { Client, QueryResult } from 'pg';

/**
 * 定义 PgClient 接口，供 Service 层依赖时使用，实现解耦。
 */
export interface PgClient {
    connect: () => Promise<void>;
    /** 仅提供 SQL 执行 I/O 能力，不包含业务知识。 */
    query: (sql: string, params?: any[]) => Promise<QueryResult>;
}

/**
 * PgClient 的工厂函数。
 */
export function createPgClient(connectionString: string): PgClient {
    if (!connectionString) {
        throw new Error("DATABASE_URL connection string is required.");
    }
    const dbClient = new Client({ connectionString });

    return {
        connect: async () => {
            await dbClient.connect();
            console.log("✅ [PG Client] Database connected successfully.");
        },
        query: async <T>(sql: string, params: any[] = []): Promise<QueryResult> => {
            try {
                // Client 只执行 I/O
                return await dbClient.query(sql, params);
            } catch (error) {
                console.error("❌ [PG Client] Query execution failed:", (error as Error).message);
                throw error; // 抛出给 Service 层处理
            }
        }
    };
}