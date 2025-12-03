use serde::Deserialize;
use bcs::from_bytes;
use sui_indexer_alt_framework::types::base_types::SuiAddress;

#[derive(Debug, Deserialize)]
pub struct SwapEvent {
    pub atob: bool,
    pub pool: SuiAddress,
    pub partner: SuiAddress,
    pub amount_in: u64,
    pub amount_out: u64,
    pub ref_amount: u64,
    pub fee_amount: u64,
    pub vault_a_amount: u64,
    pub vault_b_amount: u64,
    pub before_sqrt_price: u128,
    pub after_sqrt_price: u128,
    pub steps: u64,
}

pub fn deserialize_swap_result(event_contents: Vec<u8>) -> Result<SwapEvent, bcs::Error> {
    from_bytes::<SwapEvent>(&event_contents)
}
