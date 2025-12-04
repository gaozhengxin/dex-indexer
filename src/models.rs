use diesel::prelude::*;
use sui_indexer_alt_framework::FieldCount;
use crate::schema::transaction_digests;
use crate::schema::cetus_swap;
use rust_decimal::Decimal;

#[derive(Insertable, Debug, Clone, FieldCount)]
#[diesel(table_name = transaction_digests)]
pub struct StoredTransactionDigest {
    pub tx_digest: String,
    pub checkpoint_sequence_number: i64,
}

#[derive(Insertable, Debug, Clone, FieldCount)]
#[diesel(table_name = cetus_swap)]
pub struct StoredCetusSwap {
    pub tx_digest: String,
    pub event_id: i64,
    pub pool: String,
    pub amount_a_in: Decimal,
    pub amount_a_out: Decimal,
    pub amount_b_in: Decimal,
    pub amount_b_out: Decimal,
    pub fee_amount_a: Decimal,
    pub fee_amount_b: Decimal,
    pub timestamp: i64,
}