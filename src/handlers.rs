use std::{hash::Hash, sync::Arc, time::UNIX_EPOCH};
use anyhow::Result;
use async_trait::async_trait;
use diesel::{IntoSql, expression::AsExpression, serialize::ToSql};
use sui_indexer_alt_framework::{pipeline::Processor, types::{base_types::ObjectID, event::Event, full_checkpoint_content::Checkpoint}};

use crate::models::StoredCetusSwap;
use crate::schema::cetus_swap::dsl::*;
use crate::sui_types::deserialize_swap_result;

use diesel_async::RunQueryDsl;
use sui_indexer_alt_framework::{
    postgres::{Connection, Db},
    pipeline::sequential::Handler,
};
use rust_decimal::Decimal;


pub struct TransactionDigestHandler;

#[async_trait]
impl Processor for TransactionDigestHandler {
    const NAME: &'static str = "transaction_digest_handler";

    type Value = StoredCetusSwap;

    async fn process(&self, checkpoint: &Arc<Checkpoint>) -> Result<Vec<Self::Value>> {
        let block_timestamp = checkpoint.summary.timestamp().duration_since(UNIX_EPOCH).unwrap().as_secs();
        let e_package_cetus: ObjectID = ObjectID::from_hex_literal("0x1eabed72c53feb3805120a081dc15963c204dc8d091542592abaf7a35689b2fb").expect("Invalid hex literal for PACKAGE_ID_1");
        let e_module_cetus_pool: &str = "pool";
        let e_name_swap: &str = "SwapEvent";
        let checkpoint_seq = checkpoint.summary.sequence_number as i64;

        let events: Vec<StoredCetusSwap> = checkpoint.transactions.iter().flat_map(|tx| {
            let tx_digest_value = tx.transaction.digest().to_string();
            let filtered_events = match &tx.events {
                Some(events) => {
                    events.data
                    .iter()
                    .enumerate()
                    .filter(|(index, event)| {
                        let e_package = ObjectID::from_address(event.type_.address);
                        let binding = event.type_.module_id();
                        let e_module = binding.name().as_str();
                        let e_name = event.type_.name.as_str();

                        e_package.eq(&e_package_cetus) && e_module == e_module_cetus_pool && e_name == e_name_swap
                    })
                    .collect::<Vec<_>>()
                },
                None => Vec::new(),
            };
            filtered_events
            .into_iter()
            .flat_map(|(index, event)| {
                let opt = match deserialize_swap_result(event.contents.clone()) {
                    Ok(res)=>Some(StoredCetusSwap {
                        tx_digest: tx_digest_value.clone(),
                        event_id: index as i64,
                        pool: res.pool.to_string(),
                        fee_amount_a: if res.atob { Decimal::from(res.fee_amount) } else { Decimal::from(0) },
                        fee_amount_b: if res.atob { Decimal::from(0) } else { Decimal::from(res.fee_amount) },
                        timestamp: block_timestamp as i64
                    }),
                    Err(e) => {
                        println!("deserialize_swap_result error: {:?}", e);
                        None
                    }
                };
                opt
            })
            .collect::<Vec<StoredCetusSwap>>()
        }).collect::<Vec<_>>();

        let message = format!(
            "process checkpoint: {} | cetus swap events: {}",
            checkpoint_seq,
            events.len()
        );
        print!("\r {}", message);

        Ok(events)
    }
    
    #[doc = " How much concurrency to use when processing checkpoint data."]
    const FANOUT:usize = 10;
}

#[async_trait::async_trait]
impl Handler for TransactionDigestHandler {
    type Store = Db;
    type Batch = Vec<Self::Value>;

    fn batch(&self, batch: &mut Self::Batch, values: std::vec::IntoIter<Self::Value>) {
        batch.extend(values);
    }

    async fn commit<'a>(
        &self,
        batch: &Self::Batch,
        conn: &mut Connection<'a>,
    ) -> Result<usize> {
        let inserted = diesel::insert_into(cetus_swap)
            .values(batch)
            .execute(conn)
            .await?;

        Ok(inserted)
    }
    
    const MIN_EAGER_ROWS: usize = 50;
    
    const MAX_BATCH_CHECKPOINTS: usize = 5 * 60;
}