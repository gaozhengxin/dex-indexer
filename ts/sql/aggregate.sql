INSERT INTO public.cetus_swap_daily_summary (
    pool, 
    date, 
    total_a_in, 
    total_a_out, 
    total_b_in, 
    total_b_out, 
    total_fee_a, 
    total_fee_b
)
SELECT
    pool,
    CURRENT_DATE AS date_key,
    SUM(amount_a_in),
    SUM(amount_a_out),
    SUM(amount_b_in),
    SUM(amount_b_out),
    SUM(fee_amount_a),
    SUM(fee_amount_b)
FROM
    public.cetus_swap
WHERE
    -- 筛选过去 24 小时的数据 (86400 秒)
    timestamp >= (EXTRACT(EPOCH FROM NOW()) - 86400) 
GROUP BY
    pool

-- ** UPSERT 逻辑 **
-- 引用 UNIQUE 索引 (pool, date)
ON CONFLICT (pool, date) DO UPDATE 
SET
    -- 如果发生冲突（该 pool 今天的记录已存在），则使用新计算的值进行覆盖更新 (EXCLUDED)。
    total_a_in  = EXCLUDED.total_a_in,
    total_a_out = EXCLUDED.total_a_out,
    total_b_in  = EXCLUDED.total_b_in,
    total_b_out = EXCLUDED.total_b_out,
    total_fee_a = EXCLUDED.total_fee_a,
    total_fee_b = EXCLUDED.total_fee_b;