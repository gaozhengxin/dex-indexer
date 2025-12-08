import { createClient, RedisClientType } from 'redis';

export interface HistoricalPriceGetter {
    getHistoricalPrice: (tokenType: string, timestampSeconds: number) => Promise<string | null>;
}

let globalRedisClient: RedisClientType | null = null;

export function createRedisClient(connectionString: string): HistoricalPriceGetter {
    if (!connectionString) throw new Error("REDIS_URL connection string is required.");

    if (!globalRedisClient) {
        globalRedisClient = createClient({ url: connectionString }) as RedisClientType;

        globalRedisClient.on('error', (err) => {
            console.error('❌ [Redis Client] Connection Error:', err);
        });

        globalRedisClient.connect().then(() => {
            console.log('✅ [Redis Client] Connected successfully.');
        }).catch((err) => {
            console.error('❌ [Redis Client] Failed to connect:', err);
        });
    }

    return {
        getHistoricalPrice: async (tokenType: string, timestampSeconds: number) => {
            const key = `token:historical-price:${tokenType}:${timestampSeconds}`;
            try {
                return await globalRedisClient!.get(key);
            } catch (err) {
                console.error(`❌ [Redis Client] Failed to get price for key ${key}:`, (err as Error).message);
                return null;
            }
        }
    };
}
