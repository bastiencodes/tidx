use chrono::{DateTime, TimeZone, Utc};

use crate::tempo::{TempoBlock, TempoLog, TempoTransaction};
use crate::types::{BlockRow, LogRow, TxRow};

pub fn decode_block(block: &TempoBlock) -> BlockRow {
    let timestamp_secs = block.timestamp_u64();
    let timestamp = Utc.timestamp_opt(timestamp_secs as i64, 0).unwrap();
    let timestamp_ms = block.timestamp_millis_u64() as i64;

    BlockRow {
        num: block.number_u64() as i64,
        hash: block.hash.as_slice().to_vec(),
        parent_hash: block.parent_hash.as_slice().to_vec(),
        timestamp,
        timestamp_ms,
        gas_limit: block.gas_limit_u64() as i64,
        gas_used: block.gas_used_u64() as i64,
        miner: block.miner.as_slice().to_vec(),
        extra_data: block.extra_data.as_ref().map(|b| b.to_vec()),
    }
}

pub fn decode_transaction(tx: &TempoTransaction, block: &TempoBlock, idx: u32) -> TxRow {
    let timestamp_secs = block.timestamp_u64();
    let block_timestamp = Utc.timestamp_opt(timestamp_secs as i64, 0).unwrap();

    let nonce_key = tx
        .nonce_key
        .map(|k| k.to_be_bytes_vec())
        .unwrap_or_else(|| vec![0u8; 32]);

    let calls_json = tx.calls.as_ref().and_then(|c| serde_json::to_value(c).ok());

    TxRow {
        block_num: block.number_u64() as i64,
        block_timestamp,
        idx: idx as i32,
        hash: tx.hash.as_slice().to_vec(),
        tx_type: tx.tx_type_u8() as i16,
        from: tx.from.as_slice().to_vec(),
        to: tx.effective_to().map(|a| a.as_slice().to_vec()),
        value: tx.effective_value().to_string(),
        input: tx.effective_input().to_vec(),
        gas_limit: tx.gas.to::<u64>() as i64,
        max_fee_per_gas: tx
            .max_fee_per_gas
            .map(|v| v.to_string())
            .unwrap_or_else(|| tx.gas_price.map(|v| v.to_string()).unwrap_or("0".into())),
        max_priority_fee_per_gas: tx
            .max_priority_fee_per_gas
            .map(|v| v.to_string())
            .unwrap_or_else(|| "0".into()),
        gas_used: None,
        nonce_key,
        nonce: tx.nonce.to::<u64>() as i64,
        fee_token: tx.fee_token.map(|a| a.as_slice().to_vec()),
        fee_payer: None, // Would need to recover from fee_payer_signature
        calls: calls_json,
        call_count: tx.call_count(),
        valid_before: tx.valid_before.map(|v| v.to::<u64>() as i64),
        valid_after: tx.valid_after.map(|v| v.to::<u64>() as i64),
        signature_type: tx.signature_type(),
    }
}

pub fn decode_log(log: &TempoLog, block_timestamp: DateTime<Utc>) -> LogRow {
    let selector = log.selector().map(|s| s.as_slice()[0..4].to_vec());
    let topics: Vec<Vec<u8>> = log.topics.iter().map(|t| t.as_slice().to_vec()).collect();

    LogRow {
        block_num: log.block_number.to::<u64>() as i64,
        block_timestamp,
        log_idx: log.log_index.to::<u64>() as i32,
        tx_idx: log.transaction_index.to::<u64>() as i32,
        tx_hash: log.transaction_hash.as_slice().to_vec(),
        address: log.address.as_slice().to_vec(),
        selector,
        topics,
        data: log.data.to_vec(),
    }
}
