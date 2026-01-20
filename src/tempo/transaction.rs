use alloy::primitives::{Address, Bytes, B256, U256, U64};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TempoTransaction {
    #[serde(rename = "type")]
    pub tx_type: U64,
    pub chain_id: Option<U64>,
    pub hash: B256,
    pub from: Address,
    #[serde(default)]
    pub to: Option<Address>,
    #[serde(default)]
    pub value: Option<U256>,
    #[serde(default)]
    pub input: Option<Bytes>,
    pub gas: U64,
    #[serde(default)]
    pub gas_price: Option<U256>,
    #[serde(default)]
    pub max_fee_per_gas: Option<U256>,
    #[serde(default)]
    pub max_priority_fee_per_gas: Option<U256>,
    pub nonce: U64,
    pub block_hash: Option<B256>,
    pub block_number: Option<U64>,
    pub transaction_index: Option<U64>,

    // Tempo-specific fields (0x76 transactions)
    #[serde(default)]
    pub calls: Option<Vec<TempoCall>>,
    #[serde(default)]
    pub nonce_key: Option<U256>,
    #[serde(default)]
    pub fee_token: Option<Address>,
    #[serde(default)]
    pub valid_before: Option<U64>,
    #[serde(default)]
    pub valid_after: Option<U64>,
    #[serde(default)]
    pub signature: Option<TempoSignature>,
    #[serde(default)]
    pub fee_payer_signature: Option<TempoSignature>,

    // Legacy signature fields
    #[serde(default)]
    pub v: Option<U64>,
    #[serde(default)]
    pub r: Option<U256>,
    #[serde(default)]
    pub s: Option<U256>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TempoCall {
    #[serde(default)]
    pub to: Option<Address>,
    #[serde(default)]
    pub value: Option<U256>,
    #[serde(default)]
    pub input: Option<Bytes>,
    #[serde(default)]
    pub data: Option<Bytes>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TempoSignature {
    #[serde(rename = "type")]
    pub sig_type: Option<String>,
    #[serde(default)]
    pub r: Option<U256>,
    #[serde(default)]
    pub s: Option<U256>,
    #[serde(default)]
    pub v: Option<U64>,
    #[serde(default)]
    pub y_parity: Option<U64>,
}

impl TempoTransaction {
    pub fn tx_type_u8(&self) -> u8 {
        self.tx_type.to::<u64>() as u8
    }

    pub fn is_tempo_native(&self) -> bool {
        self.tx_type_u8() == 0x76
    }

    pub fn effective_to(&self) -> Option<Address> {
        if let Some(calls) = &self.calls
            && let Some(first) = calls.first()
        {
            return first.to;
        }
        self.to
    }

    pub fn effective_value(&self) -> U256 {
        if let Some(calls) = &self.calls {
            return calls.iter().fold(U256::ZERO, |acc, c| {
                acc + c.value.unwrap_or(U256::ZERO)
            });
        }
        self.value.unwrap_or(U256::ZERO)
    }

    pub fn effective_input(&self) -> Bytes {
        if let Some(calls) = &self.calls
            && let Some(first) = calls.first()
        {
            return first.input.clone().or(first.data.clone()).unwrap_or_default();
        }
        self.input.clone().unwrap_or_default()
    }

    pub fn call_count(&self) -> i16 {
        self.calls.as_ref().map_or(1, |c| c.len() as i16)
    }

    pub fn signature_type(&self) -> Option<i16> {
        if let Some(sig) = &self.signature {
            return match sig.sig_type.as_deref() {
                Some("secp256k1") => Some(0),
                Some("p256") => Some(1),
                Some("webauthn") => Some(2),
                _ => Some(0),
            };
        }
        Some(0)
    }
}
