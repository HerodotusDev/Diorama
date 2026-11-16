//! The venue's L1-action signature, recovered (docs: "Signing").
//!
//! The real venue reads an `/exchange` action's acting account from its
//! signature, not from a field. The hash it checks is the MessagePack
//! encoding of the action **as the client sent it** (key order and integer
//! widths are part of it), then the nonce (8 bytes big-endian), the vault
//! marker (`0x00`, or `0x01` and the vault address) and, when the body sets
//! `expiresAfter`, `0x00` and that timestamp. The client signs the hash as
//! the EIP-712 `Agent { source, connectionId }` of the phantom `Exchange`
//! domain (chain id 1337, verifying contract zero), `source` being `"a"` on
//! mainnet.
//!
//! The action is re-encoded from the body's raw JSON (read in order, never
//! through a typed struct or a sorted map), so a client that sends what it
//! sends to the venue recovers to its own key.
use crate::wire::Signature;
use alloy_primitives::{keccak256, Address, B256, U256};
use alloy_sol_types::{eip712_domain, sol, Eip712Domain, SolStruct};
use serde::de::{self, Deserialize, Deserializer, MapAccess, SeqAccess, Visitor};
use serde_json::value::RawValue;
use std::fmt;

/// The source letter of a mainnet signature (`"b"` is testnet's): the mirror
/// follows mainnet, so a client signs as it would for the real venue.
const MAINNET_SOURCE: &str = "a";

sol! {
    /// The phantom agent the venue's L1 actions are signed as.
    struct Agent {
        string source;
        bytes32 connectionId;
    }
}

/// The phantom domain every L1 action is signed under.
fn exchange_domain() -> Eip712Domain {
    eip712_domain! {
        name: "Exchange",
        version: "1",
        chain_id: 1337,
        verifying_contract: Address::ZERO,
    }
}

/// The fields of an `/exchange` body the signature covers. The action stays
/// raw: its bytes are what the client encoded.
#[derive(serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct SignedBody<'a> {
    #[serde(borrow)]
    action: &'a RawValue,
    nonce: u64,
    signature: Signature,
    #[serde(default)]
    vault_address: Option<Address>,
    #[serde(default)]
    expires_after: Option<u64>,
}

/// The address whose key signed `body` (an `/exchange` body). `Err` names
/// why nothing could be recovered: a missing or malformed field, or a
/// signature that is not a valid secp256k1 signature. A tampered body still
/// recovers, to an address that is nobody's.
pub(crate) fn recover_signer(body: &[u8]) -> Result<Address, String> {
    let signed: SignedBody<'_> = serde_json::from_slice(body)
        .map_err(|e| format!("venue-sim: the signed fields do not parse: {e}"))?;
    let digest = signing_digest(
        signed.action.get(),
        signed.nonce,
        signed.vault_address,
        signed.expires_after,
    )?;
    recover(&signed.signature, digest)
}

/// The EIP-712 digest a client signs for `action` (its raw JSON).
pub(crate) fn signing_digest(
    action: &str,
    nonce: u64,
    vault: Option<Address>,
    expires_after: Option<u64>,
) -> Result<B256, String> {
    let agent = Agent {
        source: MAINNET_SOURCE.to_string(),
        connectionId: action_hash(action, nonce, vault, expires_after)?,
    };
    Ok(agent.eip712_signing_hash(&exchange_domain()))
}

/// The venue's `connectionId`: the hash of the encoded action, the nonce and
/// the vault and expiry markers.
fn action_hash(
    action: &str,
    nonce: u64,
    vault: Option<Address>,
    expires_after: Option<u64>,
) -> Result<B256, String> {
    let node: Node = serde_json::from_str(action)
        .map_err(|e| format!("venue-sim: the action does not parse: {e}"))?;
    let mut data = Vec::new();
    node.encode(&mut data);
    data.extend_from_slice(&nonce.to_be_bytes());
    match vault {
        None => data.push(0),
        Some(vault) => {
            data.push(1);
            data.extend_from_slice(vault.as_slice());
        }
    }
    if let Some(expires_after) = expires_after {
        data.push(0);
        data.extend_from_slice(&expires_after.to_be_bytes());
    }
    Ok(keccak256(&data))
}

/// The signer of `digest`. `v` is the venue's 27/28 (0/1 is read as the bare
/// parity); `r` and `s` are hex, zero-padding optional (the Python SDK drops
/// it).
fn recover(signature: &Signature, digest: B256) -> Result<Address, String> {
    let y_parity = match signature.v {
        0 | 27 => false,
        1 | 28 => true,
        v => return Err(format!("venue-sim: signature `v` is {v}, not 27 or 28")),
    };
    let r = scalar("r", &signature.r)?;
    let s = scalar("s", &signature.s)?;
    alloy_primitives::Signature::new(r, s, y_parity)
        .recover_address_from_prehash(&digest)
        .map_err(|e| format!("venue-sim: the signature does not recover a key: {e}"))
}

fn scalar(name: &str, hex: &str) -> Result<U256, String> {
    let digits = hex.strip_prefix("0x").unwrap_or(hex);
    U256::from_str_radix(digits, 16)
        .map_err(|e| format!("venue-sim: signature `{name}` is not a hex scalar: {e}"))
}

/// A JSON value as the client's encoder sees it: object keys in their
/// written order, integers apart from floats.
enum Node {
    Nil,
    Bool(bool),
    Uint(u64),
    Int(i64),
    Float(f64),
    Str(String),
    Array(Vec<Node>),
    Map(Vec<(String, Node)>),
}

impl Node {
    /// MessagePack, each value in its most compact form — what the clients'
    /// encoders write (msgpack-python, @msgpack/msgpack): integers by
    /// magnitude, non-integers as float 64.
    fn encode(&self, out: &mut Vec<u8>) {
        match self {
            Node::Nil => out.push(0xc0),
            Node::Bool(b) => out.push(if *b { 0xc3 } else { 0xc2 }),
            Node::Uint(n) => encode_uint(*n, out),
            Node::Int(n) => encode_int(*n, out),
            Node::Float(f) => {
                out.push(0xcb);
                out.extend_from_slice(&f.to_be_bytes());
            }
            Node::Str(s) => encode_str(s, out),
            Node::Array(items) => {
                encode_len(items.len(), [0x90, 0xdc, 0xdd], 16, out);
                for item in items {
                    item.encode(out);
                }
            }
            Node::Map(entries) => {
                encode_len(entries.len(), [0x80, 0xde, 0xdf], 16, out);
                for (key, value) in entries {
                    encode_str(key, out);
                    value.encode(out);
                }
            }
        }
    }
}

fn encode_uint(n: u64, out: &mut Vec<u8>) {
    match n {
        0..=0x7f => out.push(n as u8),
        0x80..=0xff => out.extend_from_slice(&[0xcc, n as u8]),
        0x100..=0xffff => {
            out.push(0xcd);
            out.extend_from_slice(&(n as u16).to_be_bytes());
        }
        0x1_0000..=0xffff_ffff => {
            out.push(0xce);
            out.extend_from_slice(&(n as u32).to_be_bytes());
        }
        _ => {
            out.push(0xcf);
            out.extend_from_slice(&n.to_be_bytes());
        }
    }
}

fn encode_int(n: i64, out: &mut Vec<u8>) {
    if let Ok(n) = u64::try_from(n) {
        return encode_uint(n, out);
    }
    if n >= -32 {
        out.push(n as u8);
    } else if let Ok(n) = i8::try_from(n) {
        out.push(0xd0);
        out.extend_from_slice(&n.to_be_bytes());
    } else if let Ok(n) = i16::try_from(n) {
        out.push(0xd1);
        out.extend_from_slice(&n.to_be_bytes());
    } else if let Ok(n) = i32::try_from(n) {
        out.push(0xd2);
        out.extend_from_slice(&n.to_be_bytes());
    } else {
        out.push(0xd3);
        out.extend_from_slice(&n.to_be_bytes());
    }
}

fn encode_str(s: &str, out: &mut Vec<u8>) {
    let len = s.len();
    if len < 32 {
        out.push(0xa0 | len as u8);
    } else if let Ok(len) = u8::try_from(len) {
        out.extend_from_slice(&[0xd9, len]);
    } else if let Ok(len) = u16::try_from(len) {
        out.push(0xda);
        out.extend_from_slice(&len.to_be_bytes());
    } else {
        out.push(0xdb);
        out.extend_from_slice(&(len as u32).to_be_bytes());
    }
    out.extend_from_slice(s.as_bytes());
}

/// An array or map header: `fix | len` below `fix_limit`, else the 16- or
/// 32-bit length form.
fn encode_len(len: usize, [fix, wide16, wide32]: [u8; 3], fix_limit: usize, out: &mut Vec<u8>) {
    if len < fix_limit {
        out.push(fix | len as u8);
    } else if let Ok(len) = u16::try_from(len) {
        out.push(wide16);
        out.extend_from_slice(&len.to_be_bytes());
    } else {
        out.push(wide32);
        out.extend_from_slice(&(len as u32).to_be_bytes());
    }
}

impl<'de> Deserialize<'de> for Node {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        deserializer.deserialize_any(NodeVisitor)
    }
}

struct NodeVisitor;

impl<'de> Visitor<'de> for NodeVisitor {
    type Value = Node;

    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("a JSON value")
    }

    fn visit_unit<E: de::Error>(self) -> Result<Node, E> {
        Ok(Node::Nil)
    }

    fn visit_bool<E: de::Error>(self, b: bool) -> Result<Node, E> {
        Ok(Node::Bool(b))
    }

    fn visit_u64<E: de::Error>(self, n: u64) -> Result<Node, E> {
        Ok(Node::Uint(n))
    }

    fn visit_i64<E: de::Error>(self, n: i64) -> Result<Node, E> {
        Ok(Node::Int(n))
    }

    fn visit_f64<E: de::Error>(self, f: f64) -> Result<Node, E> {
        Ok(Node::Float(f))
    }

    fn visit_str<E: de::Error>(self, s: &str) -> Result<Node, E> {
        Ok(Node::Str(s.to_string()))
    }

    fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Node, A::Error> {
        let mut items = Vec::new();
        while let Some(item) = seq.next_element()? {
            items.push(item);
        }
        Ok(Node::Array(items))
    }

    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Node, A::Error> {
        let mut entries = Vec::new();
        while let Some(entry) = map.next_entry::<String, Node>()? {
            entries.push(entry);
        }
        Ok(Node::Map(entries))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Anvil's first account, the key every vector below was signed with.
    const SIGNER: &str = "0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266";

    /// Bodies exactly as a browser client posts them, signed by a TypeScript
    /// signer (viem's `signTypedData` over an @msgpack/msgpack 3 encoding):
    /// an order with a builder and a cloid, a cancel whose oid takes the
    /// 32-bit form, and a leverage update.
    const TYPESCRIPT_BODIES: [&str; 3] = [
        r#"{"action":{"type":"order","orders":[{"a":0,"b":true,"p":"105","s":"2","r":false,"t":{"limit":{"tif":"Ioc"}},"c":"0x00000000000000000000000000000abc"}],"grouping":"na","builder":{"b":"0x70997970c51812dc3a010c7d01b50e0d17dc79c8","f":50}},"nonce":1763300000000,"signature":{"r":"0xcc9fe0e868d06cbb65c1f540a65833081f1ee8fe9c8b80d18ada3ab1248159fb","s":"0x515a4a97d98ade0a92acb029f5b38de943ed62858b60e7608efd31e0c474d3ad","v":27},"vaultAddress":null,"expiresAfter":null}"#,
        r#"{"action":{"type":"cancel","cancels":[{"a":0,"o":4000000000}]},"nonce":1763300000000,"signature":{"r":"0xac95465019c3eddb91fe5c71aaaf037afec5213d6cf5fded8d607ee523334ba3","s":"0x451b4d09e092377a5a7d88419aa345d7649b9584541bea31a19c623beabb43ee","v":28},"vaultAddress":null,"expiresAfter":null}"#,
        r#"{"action":{"type":"updateLeverage","asset":0,"isCross":true,"leverage":10},"nonce":1763300000000,"signature":{"r":"0xf43118a59f3ab242a9f6674c14aba7347d52091c424cf212dcd4ed39905d7b6e","s":"0x0451dd7bc7b002717811f1163a6e216cc7895b0c2fcd2a397fa15f30318f063a","v":27},"vaultAddress":null,"expiresAfter":null}"#,
    ];

    /// Bodies signed by the venue's own Python SDK (hyperliquid-python-sdk
    /// 0.24.0, `sign_l1_action`, msgpack 1.2.4): a 64-bit oid (and an `s`
    /// the SDK does not zero-pad), a vault, a vault with `expiresAfter`, and
    /// `expiresAfter` alone.
    const PYTHON_SDK_BODIES: [&str; 4] = [
        r#"{"action":{"type":"cancel","cancels":[{"a":0,"o":131110607934}]},"nonce":1763300000000,"signature":{"r":"0x6e206355ea405ee38b19e4094fae5d774250604079e3d2d74c27b8fcddfdd957","s":"0x61564dcc92fc3d7c94b55e4da63d50f59a4f0865bca53f2b9a5482553367103","v":27},"vaultAddress":null,"expiresAfter":null}"#,
        r#"{"action":{"type":"order","orders":[{"a":0,"b":false,"p":"99.5","s":"0.25","r":true,"t":{"limit":{"tif":"Gtc"}}}],"grouping":"na"},"nonce":1763300000000,"signature":{"r":"0xc6efc75dce71715da419b74bec6da12eed452fd9480b0d1677a39e9b40c1f0c2","s":"0x6cc5723ffa801958f31fbd1dc05b9cc9e937ab45e0591a3c9cb3d8b6a66e320b","v":27},"vaultAddress":"0x00000000000000000000000000000000000000e2","expiresAfter":null}"#,
        r#"{"action":{"type":"updateLeverage","asset":0,"isCross":false,"leverage":3},"nonce":1763300000000,"signature":{"r":"0xdaae01a4dfba4ecbf40d2cfb2d64507d60106571254331d9400f062839ef4546","s":"0x3024522fca33bc02831a4b3ac0d583429552a45870afef1305d9b2c6a1de094b","v":27},"vaultAddress":"0x00000000000000000000000000000000000000e2","expiresAfter":1763300060000}"#,
        r#"{"action":{"type":"cancel","cancels":[{"a":7,"o":12}]},"nonce":1763300000000,"signature":{"r":"0xbc338249efb343c25a4e0baa17d551b9d887276147959d9d24869dae769e9072","s":"0x1e0bd3d08dbef8ad259642bedf69dec4d792e4b1231d3ce7988364efd5a21cda","v":27},"vaultAddress":null,"expiresAfter":1763300060000}"#,
    ];

    fn signer() -> Address {
        SIGNER.parse().unwrap()
    }

    #[test]
    fn a_typescript_signers_signatures_recover_its_key() {
        for body in TYPESCRIPT_BODIES {
            assert_eq!(recover_signer(body.as_bytes()), Ok(signer()), "{body}");
        }
    }

    #[test]
    fn the_python_sdks_signatures_recover_its_key() {
        for body in PYTHON_SDK_BODIES {
            assert_eq!(recover_signer(body.as_bytes()), Ok(signer()), "{body}");
        }
    }

    /// Every covered field is bound: changing the price, the nonce, the vault
    /// or the expiry recovers another address.
    #[test]
    fn a_changed_field_recovers_another_address() {
        let edits = [
            (TYPESCRIPT_BODIES[0], r#""p":"105""#, r#""p":"106""#),
            (TYPESCRIPT_BODIES[0], "1763300000000", "1763300000001"),
            (
                PYTHON_SDK_BODIES[1],
                "00000000000000000000000000000000000000e2",
                "00000000000000000000000000000000000000e3",
            ),
            (PYTHON_SDK_BODIES[3], "1763300060000", "1763300060001"),
        ];
        for (body, from, to) in edits {
            let edited = body.replacen(from, to, 1);
            assert_ne!(edited, body, "the edit applies");
            let recovered = recover_signer(edited.as_bytes()).unwrap();
            assert_ne!(recovered, signer(), "{from} -> {to}");
        }
    }

    /// The key order is the client's: the same action with two keys swapped
    /// is a different hash.
    #[test]
    fn the_hash_keeps_the_clients_key_order() {
        let written = action_hash(r#"{"type":"cancel","cancels":[]}"#, 1, None, None).unwrap();
        let swapped = action_hash(r#"{"cancels":[],"type":"cancel"}"#, 1, None, None).unwrap();
        assert_ne!(written, swapped);
    }

    #[test]
    fn integers_and_strings_take_their_compact_forms() {
        let encode = |json: &str| {
            let mut out = Vec::new();
            serde_json::from_str::<Node>(json).unwrap().encode(&mut out);
            out
        };
        assert_eq!(encode("127"), [0x7f]);
        assert_eq!(encode("128"), [0xcc, 0x80]);
        assert_eq!(encode("65536"), [0xce, 0, 1, 0, 0]);
        assert_eq!(encode("4294967296"), [0xcf, 0, 0, 0, 1, 0, 0, 0, 0]);
        assert_eq!(encode("-1"), [0xff]);
        assert_eq!(encode("-33"), [0xd0, 0xdf]);
        assert_eq!(encode("1.5"), [0xcb, 0x3f, 0xf8, 0, 0, 0, 0, 0, 0]);
        assert_eq!(encode("null"), [0xc0]);
        let long = format!("\"{}\"", "x".repeat(32));
        assert_eq!(encode(&long)[..2], [0xd9, 32]);
        assert_eq!(encode(r#"{"a":[true]}"#), [0x81, 0xa1, b'a', 0x91, 0xc3]);
    }

    #[test]
    fn a_body_without_the_signed_fields_is_refused_by_name() {
        let refusal = recover_signer(br#"{"action":{"type":"cancel","cancels":[]}}"#).unwrap_err();
        assert!(refusal.contains("missing field `nonce`"), "{refusal}");
        let bad_v = TYPESCRIPT_BODIES[2].replacen(r#""v":27"#, r#""v":29"#, 1);
        let refusal = recover_signer(bad_v.as_bytes()).unwrap_err();
        assert!(refusal.contains("not 27 or 28"), "{refusal}");
    }
}
