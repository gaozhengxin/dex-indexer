// @generated automatically by Diesel CLI.

diesel::table! {
    cetus_swap (tx_digest, event_id) {
        tx_digest -> Text,
        event_id -> Int8,
        pool -> Text,
        amount_a_in -> Numeric,
        amount_a_out -> Numeric,
        amount_b_in -> Numeric,
        amount_b_out -> Numeric,
        fee_amount_a -> Numeric,
        fee_amount_b -> Numeric,
        timestamp -> Int8,
    }
}

diesel::table! {
    transaction_digests (tx_digest) {
        tx_digest -> Text,
        checkpoint_sequence_number -> Int8,
    }
}

diesel::table! {
    watermarks (pipeline) {
        pipeline -> Text,
        epoch_hi_inclusive -> Int8,
        checkpoint_hi_inclusive -> Int8,
        tx_hi -> Int8,
        timestamp_ms_hi_inclusive -> Int8,
        reader_lo -> Int8,
        pruner_timestamp -> Timestamp,
        pruner_hi -> Int8,
    }
}

diesel::allow_tables_to_appear_in_same_query!(
    cetus_swap,
    transaction_digests,
    watermarks,
);
