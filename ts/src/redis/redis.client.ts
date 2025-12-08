import { createClient, RedisClientType } from 'redis';

export interface HistoricalPriceGetter {
    checkConnection: () => Promise<void>;
    getHistoricalPrice: (tokenType: string, timestampSeconds: number) => Promise<string | null>;
}

export function createRedisClient(connectionString: string): HistoricalPriceGetter {
    if (!connectionString) throw new Error("REDIS_URL connection string is required.");

    const redisClient: RedisClientType = createClient({ url: connectionString }) as RedisClientType;
    let isConnected = false;

    redisClient.on('error', (err) => {
        console.error('❌ [Redis Client] Connection Error:', err);
        isConnected = false;
    });

    return {
        checkConnection: async () => {
            if (!isConnected) {
                await redisClient.connect();
                isConnected = true;
                console.log("✅ [Redis Client] Connected.");
            }
        },
        getHistoricalPrice: async (tokenType: string, timestampSeconds: number): Promise<string | null> => {
            if (!isConnected) {
                await redisClient.connect();
                isConnected = true;
            }
            const key = `token:historical-price:${tokenType}:${timestampSeconds}`;
            try {
                const price = await redisClient.get(key);
                return price;
            } catch (err) {
                console.error(`❌ [Redis Client] Failed to get price for key ${key}:`, (err as Error).message);
                return null;
            }
        }
    };
}
