import { createClient, RedisClientType } from 'redis';

export interface HistoricalPriceGetter {
    getHistoricalPrice: (tokenType: string, timestampSeconds: number) => Promise<string | null>;
    getCoinDecimals: (coinId: string) => Promise<number | null>;
}

let globalRedisClient: RedisClientType | null = null;

class SimpleSizeLimitedCache<V> {
    private cache: Map<string, V>;
    private maxSize: number;

    constructor(maxSize: number) {
        this.maxSize = maxSize;
        this.cache = new Map<string, V>();
    }

    public get(key: string): V | undefined {
        // 由于我们不实现完整的 LRU (需要更新键的位置)，这里的 get 只是简单获取
        return this.cache.get(key);
    }

    public set(key: string, value: V): void {
        // 1. 设置新值
        this.cache.set(key, value);

        // 2. 检查并执行淘汰策略 (FIFO/LIFO 近似)
        if (this.cache.size > this.maxSize) {
            // 获取 Map 中的第一个键 (Map 迭代器返回插入顺序，近似 FIFO)
            const keyToDelete = this.cache.keys().next().value;
            if (keyToDelete) {
                this.cache.delete(keyToDelete);
                // console.log(`[Cache] Evicted key: ${keyToDelete}`); // 调试用
            }
        }
    }

    public has(key: string): boolean {
        return this.cache.has(key);
    }
}

const PRICE_CACHE_MAX_SIZE = 50000;
const DECIMALS_CACHE_MAX_SIZE = 10000;

const priceCache = new SimpleSizeLimitedCache<string | null>(PRICE_CACHE_MAX_SIZE);
const decimalsCache = new SimpleSizeLimitedCache<number | null>(DECIMALS_CACHE_MAX_SIZE);

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
        getHistoricalPrice: async (tokenType: string, timestampSeconds: number): Promise<string | null> => {
            const key = `${tokenType}:${timestampSeconds}`;

            // 1. 检查内存缓存
            if (priceCache.has(key)) {
                return priceCache.get(key) || null;
            }

            // 2. 检查 Redis
            const redisKey = `token:historical-price:${tokenType}:${timestampSeconds}`;
            try {
                const price = await globalRedisClient!.get(redisKey);

                // 3. 写入内存缓存
                priceCache.set(key, price);
                return price;

            } catch (err) {
                console.error(`❌ [Redis Client] Failed to get price for key ${redisKey}:`, (err as Error).message);
                return null;
            }
        },

        getCoinDecimals: async (coinId: string): Promise<number | null> => {

            // 1. 检查内存缓存
            if (decimalsCache.has(coinId)) {
                return decimalsCache.get(coinId) || null;
            }

            // 2. 检查 Redis
            const redisKey = `coin:${coinId}`;
            try {
                const jsonStr = await globalRedisClient!.get(redisKey);

                let decimals: number | null = null;
                if (jsonStr) {
                    const obj = JSON.parse(jsonStr);
                    if (typeof obj.decimals === 'number') {
                        decimals = obj.decimals;
                    }
                }

                // 3. 写入内存缓存
                decimalsCache.set(coinId, decimals);
                return decimals;

            } catch (err) {
                console.error(`❌ [Redis Client] Failed to get decimals for key ${redisKey}:`, (err as Error).message);
                decimalsCache.set(coinId, null);
                return null;
            }
        }
    };
}
