use chrono::{DateTime, TimeZone, Utc};

use crate::tempo::{TempoBlock, TempoLog, TempoReceipt, TempoTransaction};
use crate::types::{BlockRow, LogRow, ReceiptRow, TxRow};

pub fn timestamp_from_secs(secs: u64) -> DateTime<Utc> {
    Utc.timestamp_opt(secs as i64, 0)
        .single()
        .unwrap_or_else(|| Utc.timestamp_opt(0, 0).single().unwrap())
}

pub fn decode_block(block: &TempoBlock) -> BlockRow {
    let timestamp_secs = block.timestamp_u64();
    let timestamp = timestamp_from_secs(timestamp_secs);
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
    let block_timestamp = timestamp_from_secs(block.timestamp_u64());

    let nonce_key = tx
        .nonce_key.map_or_else(|| vec![0u8; 32], |k| k.to_be_bytes_vec());

    let calls_json = tx.calls.as_ref().and_then(|c| serde_json::to_value(c).ok());

    TxRow {
        block_num: block.number_u64() as i64,
        block_timestamp,
        idx: idx as i32,
        hash: tx.hash.as_slice().to_vec(),
        tx_type: i16::from(tx.tx_type_u8()),
        from: tx.from.as_slice().to_vec(),
        to: tx.effective_to().map(|a| a.as_slice().to_vec()),
        value: tx.effective_value().to_string(),
        input: tx.effective_input().to_vec(),
        gas_limit: tx.gas.to::<u64>() as i64,
        max_fee_per_gas: tx
            .max_fee_per_gas.map_or_else(|| tx.gas_price.map_or("0".into(), |v| v.to_string()), |v| v.to_string()),
        max_priority_fee_per_gas: tx
            .max_priority_fee_per_gas.map_or_else(|| "0".into(), |v| v.to_string()),
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
    let selector = log.selector().map(|s| s.as_slice().to_vec());
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

pub fn decode_receipt(receipt: &TempoReceipt, block_timestamp: DateTime<Utc>) -> ReceiptRow {
    ReceiptRow {
        block_num: receipt.block_number.to::<u64>() as i64,
        block_timestamp,
        tx_idx: receipt.transaction_index.to::<u64>() as i32,
        tx_hash: receipt.transaction_hash.as_slice().to_vec(),
        from: receipt.from.as_slice().to_vec(),
        to: receipt.to.map(|a| a.as_slice().to_vec()),
        contract_address: receipt.contract_address.map(|a| a.as_slice().to_vec()),
        gas_used: receipt.gas_used.to::<u64>() as i64,
        cumulative_gas_used: receipt.cumulative_gas_used.to::<u64>() as i64,
        effective_gas_price: receipt.effective_gas_price.map(|p| p.to_string()),
        status: receipt.status.map(|s| s.to::<u64>() as i16),
        fee_payer: receipt.fee_payer.map(|a| a.as_slice().to_vec()),
    }
}
