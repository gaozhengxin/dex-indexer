CREATE TABLE public.cetus_liquidity_snapshot (
    pool TEXT NOT NULL,

    amount_a NUMERIC NOT NULL DEFAULT 0,

    amount_b NUMERIC NOT NULL DEFAULT 0,

    type_a TEXT NOT NULL,

    type_b TEXT NOT NULL,

    "timestamp" BIGINT NOT NULL
);

CREATE INDEX idx_liquidity_pool_timestamp
ON public.cetus_liquidity_snapshot (pool, "timestamp");