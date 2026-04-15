use alloy::consensus::transaction::Recovered;
use alloy::consensus::{Transaction as TransactionTrait, Typed2718};
use alloy::network::{ReceiptResponse, TransactionResponse};
use chrono::{DateTime, TimeZone, Utc};
use tempo_alloy::primitives::transaction::SignatureType;

use crate::chain::{Block, Log, Receipt, TempoTxEnvelope, Transaction};
use crate::types::{BlockRow, LogRow, ReceiptRow, TxRow};

pub fn timestamp_from_secs(secs: u64) -> DateTime<Utc> {
    Utc.timestamp_opt(secs as i64, 0)
        .single()
        .unwrap_or_else(|| Utc.timestamp_opt(0, 0).single().unwrap())
}

#[derive(Debug)]
struct TxChainFields {
    nonce_key: Vec<u8>,
    fee_token: Option<Vec<u8>>,
    calls_json: Option<serde_json::Value>,
    call_count: i16,
    valid_before: Option<i64>,
    valid_after: Option<i64>,
    signature_type: Option<i16>,
}

fn standard_evm_tx_fields(tx_type: u8) -> Option<TxChainFields> {
    // EIP-2718 transaction types seen on Ethereum-family chains.
    // Keep Tempo-only columns nullable/defaulted for compatibility.
    if matches!(tx_type, 0x0..=0x3) {
        Some(TxChainFields {
            nonce_key: vec![0u8; 32],
            fee_token: None,
            calls_json: None,
            call_count: 1,
            valid_before: None,
            valid_after: None,
            signature_type: None,
        })
    } else {
        None
    }
}

fn fallback_tx_fields() -> TxChainFields {
    TxChainFields {
        nonce_key: vec![0u8; 32],
        fee_token: None,
        calls_json: None,
        call_count: 1,
        valid_before: None,
        valid_after: None,
        signature_type: Some(0),
    }
}

pub fn decode_block(block: &Block) -> BlockRow {
    let timestamp_secs = block.header.timestamp;
    let timestamp = timestamp_from_secs(timestamp_secs);
    let timestamp_ms = (timestamp_secs * 1000) as i64;

    BlockRow {
        num: block.header.number as i64,
        hash: block.header.hash.as_slice().to_vec(),
        parent_hash: block.header.parent_hash.as_slice().to_vec(),
        timestamp,
        timestamp_ms,
        gas_limit: block.header.gas_limit as i64,
        gas_used: block.header.gas_used as i64,
        miner: block.header.beneficiary.as_slice().to_vec(),
        extra_data: Some(block.header.extra_data.to_vec()),
    }
}

pub fn decode_transaction(tx: &Transaction, block: &Block, idx: u32) -> TxRow {
    let block_timestamp = timestamp_from_secs(block.header.timestamp);
    let inner: &Recovered<TempoTxEnvelope> = &tx.inner;
    let tx_type = inner.ty() as u8;

    // Extract Tempo-specific fields for AA txs, otherwise use generic EVM defaults.
    let chain_fields = if let TempoTxEnvelope::AA(aa_signed) = inner.as_ref() {
        let tempo_tx = aa_signed.tx();
        TxChainFields {
            nonce_key: tempo_tx.nonce_key.to_be_bytes_vec(),
            fee_token: tempo_tx.fee_token.map(|a| a.as_slice().to_vec()),
            calls_json: serde_json::to_value(&tempo_tx.calls).ok(),
            call_count: tempo_tx.calls.len() as i16,
            valid_before: tempo_tx.valid_before.map(|v| v as i64),
            valid_after: tempo_tx.valid_after.map(|v| v as i64),
            signature_type: Some(match aa_signed.signature().signature_type() {
                SignatureType::Secp256k1 => 0,
                SignatureType::P256 => 1,
                SignatureType::WebAuthn => 2,
            }),
        }
    } else if let Some(fields) = standard_evm_tx_fields(tx_type) {
        fields
    } else {
        tracing::debug!(
            tx_type,
            tx_hash = %tx.tx_hash(),
            "Unknown non-Tempo transaction type; applying fallback Tempo-compatible defaults"
        );
        fallback_tx_fields()
    };

    TxRow {
        block_num: block.header.number as i64,
        block_timestamp,
        idx: idx as i32,
        hash: tx.tx_hash().as_slice().to_vec(),
        tx_type: tx_type as i16,
        from: inner.signer().as_slice().to_vec(),
        to: inner.to().map(|a| a.as_slice().to_vec()),
        value: inner.value().to_string(),
        input: inner.input().to_vec(),
        gas_limit: inner.gas_limit() as i64,
        max_fee_per_gas: inner.max_fee_per_gas().to_string(),
        max_priority_fee_per_gas: inner.max_priority_fee_per_gas().map_or("0".into(), |v| v.to_string()),
        gas_used: None,
        nonce_key: chain_fields.nonce_key,
        nonce: inner.nonce() as i64,
        fee_token: chain_fields.fee_token,
        fee_payer: None, // Recovered from receipt
        calls: chain_fields.calls_json,
        call_count: chain_fields.call_count,
        valid_before: chain_fields.valid_before,
        valid_after: chain_fields.valid_after,
        signature_type: chain_fields.signature_type,
    }
}

pub fn decode_log(log: &Log, block_timestamp: DateTime<Utc>) -> LogRow {
    let topics = log.topics();
    let selector = topics.first().map(|s| s.as_slice().to_vec());

    LogRow {
        block_num: log.block_number.unwrap_or(0) as i64,
        block_timestamp,
        log_idx: log.log_index.unwrap_or(0) as i32,
        tx_idx: log.transaction_index.unwrap_or(0) as i32,
        tx_hash: log
            .transaction_hash
            .map(|h| h.as_slice().to_vec())
            .unwrap_or_default(),
        address: log.address().as_slice().to_vec(),
        selector,
        topic0: topics.first().map(|t| t.as_slice().to_vec()),
        topic1: topics.get(1).map(|t| t.as_slice().to_vec()),
        topic2: topics.get(2).map(|t| t.as_slice().to_vec()),
        topic3: topics.get(3).map(|t| t.as_slice().to_vec()),
        data: log.data().data.to_vec(),
    }
}

/// Enrich transaction rows with fields that come from receipts (gas_used, fee_payer).
/// Must be called after both txs and receipts are decoded.
pub fn enrich_txs_from_receipts(txs: &mut [TxRow], receipts: &[ReceiptRow]) {
    use std::collections::HashMap;
    let receipt_map: HashMap<(i64, i32), &ReceiptRow> = receipts
        .iter()
        .map(|r| ((r.block_num, r.tx_idx), r))
        .collect();
    for tx in txs.iter_mut() {
        if let Some(r) = receipt_map.get(&(tx.block_num, tx.idx)) {
            tx.gas_used = Some(r.gas_used);
            tx.fee_payer = r.fee_payer.clone();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_tx(block_num: i64, idx: i32) -> TxRow {
        TxRow {
            block_num,
            idx,
            ..Default::default()
        }
    }

    fn make_receipt(block_num: i64, tx_idx: i32, gas_used: i64, fee_payer: Option<Vec<u8>>) -> ReceiptRow {
        ReceiptRow {
            block_num,
            tx_idx,
            gas_used,
            fee_payer,
            ..Default::default()
        }
    }

    #[test]
    fn enrich_sets_gas_used_and_fee_payer() {
        let mut txs = vec![make_tx(1, 0), make_tx(1, 1)];
        let receipts = vec![
            make_receipt(1, 0, 21000, Some(vec![0xaa; 20])),
            make_receipt(1, 1, 50000, Some(vec![0xbb; 20])),
        ];

        enrich_txs_from_receipts(&mut txs, &receipts);

        assert_eq!(txs[0].gas_used, Some(21000));
        assert_eq!(txs[0].fee_payer, Some(vec![0xaa; 20]));
        assert_eq!(txs[1].gas_used, Some(50000));
        assert_eq!(txs[1].fee_payer, Some(vec![0xbb; 20]));
    }

    #[test]
    fn enrich_leaves_unmatched_txs_as_none() {
        let mut txs = vec![make_tx(1, 0), make_tx(2, 0)];
        let receipts = vec![make_receipt(1, 0, 21000, None)];

        enrich_txs_from_receipts(&mut txs, &receipts);

        assert_eq!(txs[0].gas_used, Some(21000));
        assert_eq!(txs[1].gas_used, None);
        assert_eq!(txs[1].fee_payer, None);
    }

    #[test]
    fn enrich_empty_receipts_is_noop() {
        let mut txs = vec![make_tx(1, 0)];
        enrich_txs_from_receipts(&mut txs, &[]);
        assert_eq!(txs[0].gas_used, None);
    }

    #[test]
    fn enrich_empty_txs_is_noop() {
        let mut txs: Vec<TxRow> = vec![];
        let receipts = vec![make_receipt(1, 0, 21000, None)];
        enrich_txs_from_receipts(&mut txs, &receipts);
        assert!(txs.is_empty());
    }

    #[test]
    fn enrich_multi_block_batch() {
        let mut txs = vec![
            make_tx(10, 0),
            make_tx(10, 1),
            make_tx(11, 0),
        ];
        let receipts = vec![
            make_receipt(10, 0, 21000, Some(vec![0x01; 20])),
            make_receipt(10, 1, 42000, None),
            make_receipt(11, 0, 63000, Some(vec![0x02; 20])),
        ];

        enrich_txs_from_receipts(&mut txs, &receipts);

        assert_eq!(txs[0].gas_used, Some(21000));
        assert_eq!(txs[0].fee_payer, Some(vec![0x01; 20]));
        assert_eq!(txs[1].gas_used, Some(42000));
        assert_eq!(txs[1].fee_payer, None);
        assert_eq!(txs[2].gas_used, Some(63000));
        assert_eq!(txs[2].fee_payer, Some(vec![0x02; 20]));
    }

    #[test]
    fn standard_evm_fields_supported_types() {
        for tx_type in [0u8, 1, 2, 3] {
            let fields = standard_evm_tx_fields(tx_type).expect("standard tx type must be supported");
            assert_eq!(fields.nonce_key.len(), 32);
            assert!(fields.fee_token.is_none());
            assert!(fields.calls_json.is_none());
            assert_eq!(fields.call_count, 1);
            assert!(fields.valid_before.is_none());
            assert!(fields.valid_after.is_none());
            assert!(fields.signature_type.is_none());
        }
    }

    #[test]
    fn standard_evm_fields_rejects_unknown_types() {
        assert!(standard_evm_tx_fields(0x76).is_none());
        assert!(standard_evm_tx_fields(0x7f).is_none());
    }
}

pub fn decode_receipt(receipt: &Receipt, block_timestamp: DateTime<Utc>) -> ReceiptRow {
    ReceiptRow {
        block_num: receipt.block_number().unwrap_or(0) as i64,
        block_timestamp,
        tx_idx: receipt.transaction_index().unwrap_or(0) as i32,
        tx_hash: receipt.transaction_hash().as_slice().to_vec(),
        from: receipt.from().as_slice().to_vec(),
        to: receipt.to().map(|a| a.as_slice().to_vec()),
        contract_address: receipt.contract_address().map(|a| a.as_slice().to_vec()),
        gas_used: receipt.gas_used() as i64,
        cumulative_gas_used: receipt.cumulative_gas_used() as i64,
        effective_gas_price: Some(receipt.effective_gas_price().to_string()),
        status: if receipt.status() { Some(1) } else { Some(0) },
        fee_payer: None,
    }
}
