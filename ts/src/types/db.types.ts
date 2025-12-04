export interface LiquiditySnapshotRecord {
    pool: string;
    amount_a: number;
    amount_b: number;
    type_a: string; // Coin Type
    type_b: string; // Coin Type
    timestamp: number; // 秒时间戳
}