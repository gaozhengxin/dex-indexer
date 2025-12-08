// src/redis/redis.client.ts
import { createClient, RedisClientType } from 'redis';

/**
 * 定义 HistoricalPriceGetter 接口，供 Service 层依赖时使用。
 * 这个接口包含了业务层所需的关键 Redis 操作。
 * 在 Rust 代码中，这对应于 HistoricalPriceGetter trait。
 */
export interface HistoricalPriceGetter {
    /** 检查 Redis 连接状态 */
    checkConnection: () => Promise<void>;
    /**
     * 根据 tokenType 和对齐后的时间戳（秒），从 Redis 获取价格。
     * @param tokenType 完整的 Coin Type 字符串
     * @param timestampSeconds 对齐到分钟的秒时间戳
     * @returns 价格字符串或 null
     */
    getHistoricalPrice: (tokenType: string, timestampSeconds: number) => Promise<string | null>;
}

/**
 * RedisClient 的工厂函数。
 */
export function createRedisClient(connectionString: string): HistoricalPriceGetter {
    if (!connectionString) {
        throw new Error("REDIS_URL connection string is required.");
    }

    // 使用 RedisClientType 来保持类型安全
    const redisClient: RedisClientType = createClient({
        url: connectionString,
    }) as RedisClientType;

    // 连接失败处理
    redisClient.on('error', (err) => {
        console.error('❌ [Redis Client] Connection Error:', err);
    });

    return {
        checkConnection: async () => {
            await redisClient.connect();
            console.log("✅ [Redis Client] Database connected successfully.");
        },

        getHistoricalPrice: async (tokenType: string, timestampSeconds: number): Promise<string | null> => {
            const key = `token:historical-price:${tokenType}:${timestampSeconds}`;

            try {
                // Redis Client 只执行 I/O，业务逻辑 (Key 构造) 留给 Service 或 Trait 决定。
                // 在这个 Client 层，我们只关注 key 的构造和 GET I/O。
                const price = await redisClient.get(key);
                return price;
            } catch (error) {
                console.error(`❌ [Redis Client] Failed to get price for key ${key}:`, (error as Error).message);
                throw error; // 抛出给 Service 层处理
            }
        }
    };
}