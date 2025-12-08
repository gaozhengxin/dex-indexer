CREATE TABLE public.cetus_swap_daily_summary (
    -- 交易池地址
    pool TEXT NOT NULL,

    date DATE NOT NULL,

    total_a_in NUMERIC NOT NULL DEFAULT 0,
    total_a_out NUMERIC NOT NULL DEFAULT 0,

    total_b_in NUMERIC NOT NULL DEFAULT 0,
    total_b_out NUMERIC NOT NULL DEFAULT 0,

    total_fee_a NUMERIC NOT NULL DEFAULT 0,
    total_fee_b NUMERIC NOT NULL DEFAULT 0
);

CREATE UNIQUE INDEX idx_cetus_swap_daily_summary_pool_date_unique 
ON public.cetus_swap_daily_summary (pool, date);