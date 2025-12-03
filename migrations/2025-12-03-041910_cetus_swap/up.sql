-- Your SQL goes here
CREATE TABLE cetus_swap (
    tx_digest TEXT NOT NULL,
    event_id BIGINT NOT NULL,
    pool TEXT NOT NULL,
    fee_amount_a NUMERIC NOT NULL,
    fee_amount_b NUMERIC NOT NULL,
    timestamp BIGINT NOT NULL,
    PRIMARY KEY (tx_digest, event_id)
);