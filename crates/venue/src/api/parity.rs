//! Parity with the real venue: every info response and websocket frame the
//! frontend consumes is compared, shape for shape, against a sample captured
//! live from the real API (`fixtures/real.json`, arrays trimmed to two items).
//!
//! The comparison is structural: the same keys, the same JSON types, at every
//! depth. `null` matches anything (the venue nulls fields the sim fills and the
//! other way round), an empty array matches any array, and a key the venue
//! itself omits at times (`cloid`, `builderFee`) may be absent. Values are not
//! compared: the real sample is one moment of one account.
use super::ws::{pushes, request_reply, Live};
use super::{info, ws_codec, ApiState};
use crate::feed::MarketFeed;
use crate::sim::order::{Order, Tif};
use crate::sim::{Deal, DealClass};
use crate::wire::{test_ctx, L2Book, Level};
use alloy_primitives::Address;
use axum::body::Bytes;
use axum::extract::State;
use rust_decimal::Decimal;
use serde_json::Value;
use std::sync::Arc;
use std::time::Duration;

const REAL: &str = include_str!("fixtures/real.json");

fn real() -> Value {
    serde_json::from_str(REAL).expect("fixture")
}

fn kind(v: &Value) -> &'static str {
    match v {
        Value::Null => "null",
        Value::Bool(_) => "bool",
        Value::Number(_) => "number",
        Value::String(_) => "string",
        Value::Array(_) => "array",
        Value::Object(_) => "object",
    }
}

/// `controlOwned` and `fundingOwned` are the mirror's own marks (a scenario
/// owns the figure); the venue has no such field and no frontend reads them.
const OPTIONAL: [&str; 9] = [
    "controlOwned",
    "fundingOwned",
    "cloid",
    "builderFee",
    "twapId",
    "isSnapshot",
    "premium",
    "impactPxs",
    "midPx",
];

fn same_shape(real: &Value, sim: &Value, path: &str) {
    if real.is_null() || sim.is_null() {
        return;
    }
    assert_eq!(
        kind(real),
        kind(sim),
        "{path}: the venue sends a {} and the sim a {}",
        kind(real),
        kind(sim)
    );
    match (real, sim) {
        (Value::Object(r), Value::Object(s)) => {
            for key in r.keys() {
                assert!(
                    s.contains_key(key) || OPTIONAL.contains(&key.as_str()),
                    "{path}: the sim is missing `{key}`"
                );
            }
            for key in s.keys() {
                assert!(
                    r.contains_key(key) || OPTIONAL.contains(&key.as_str()),
                    "{path}: the sim sends `{key}`, the venue does not"
                );
            }
            for (key, rv) in r {
                if let Some(sv) = s.get(key) {
                    same_shape(rv, sv, &format!("{path}.{key}"));
                }
            }
        }
        (Value::Array(r), Value::Array(s)) => {
            // A tuple (`["", {...}]`, `[[px...], [...]]`) matches position by
            // position, a list matches its first item.
            if !r.is_empty() && r.len() == s.len() && r.len() <= 3 {
                for (i, (rv, sv)) in r.iter().zip(s).enumerate() {
                    same_shape(rv, sv, &format!("{path}[{i}]"));
                }
            } else if let (Some(rv), Some(sv)) = (r.first(), s.first()) {
                same_shape(rv, sv, &format!("{path}[0]"));
            }
        }
        _ => {}
    }
}

fn book(coin: &str) -> L2Book {
    let level = |px: &str, sz: &str| Level {
        px: px.parse().unwrap(),
        sz: sz.parse().unwrap(),
        n: 2,
    };
    L2Book {
        coin: coin.into(),
        time: crate::now_ms(),
        levels: [
            vec![level("100", "50"), level("99", "50"), level("98", "50")],
            vec![level("101", "50"), level("102", "50"), level("103", "50")],
        ],
        spread: None,
    }
}

/// A venue with one funded account that holds a position, a resting order, a
/// fill and a funding payment.
#[allow(clippy::too_many_lines)] // one fixture world: funding, position, resting order, fill, funding hour
fn world() -> (Arc<ApiState>, Address) {
    let feed = Arc::new(MarketFeed::memory(vec!["ETH".to_string()]));
    let now = crate::now_ms();
    feed.put_meta(crate::sim::meta_with_table("ETH", 10), now);
    feed.put_oi_caps(Vec::new());
    feed.put_ctx("ETH".into(), crate::wire::funding_ctx("101", "101"), now);
    feed.put_book(book("ETH"), now);
    let _ = test_ctx;
    let api = crate::feed::rest::RestClient::new(&["http://127.0.0.1:1".to_string()]).unwrap();
    let state = Arc::new(
        ApiState::new(
            feed.clone(),
            Arc::new(crate::sim::Sim::memory()),
            api,
            None,
            None,
            Duration::from_secs(1),
            Duration::from_secs(1),
        )
        .unwrap(),
    );
    let user = Address::repeat_byte(0xaa);
    state
        .sim
        .deal(
            &feed,
            &Deal {
                address: user,
                class: DealClass::Perp,
                coin: None,
                amount: Decimal::from(10_000),
            },
        )
        .unwrap();
    let order = |is_buy: bool, size: &str, px: Option<&str>, tif: Tif| Order {
        coin: "ETH".into(),
        is_buy,
        size: size.parse().unwrap(),
        limit_px: px.map(|p| p.parse().unwrap()),
        tif,
        reduce_only: false,
        cloid: Some(alloy_primitives::B128::repeat_byte(0x11)),
        trigger: None,
    };
    state
        .sim
        .order(&feed, user, &order(true, "1", None, Tif::Ioc))
        .unwrap();
    state
        .sim
        .place(&feed, user, &order(true, "1", Some("90"), Tif::Gtc))
        .unwrap();
    // An hour passes: one funding payment.
    state.sim.advance(&feed, 3600, 1).unwrap();
    (state, user)
}

async fn info_json(state: &Arc<ApiState>, body: Value) -> Value {
    let response = info::info(State(state.clone()), Bytes::from(body.to_string()))
        .await
        .expect("info");
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    serde_json::from_slice(&bytes).expect("json body")
}

#[tokio::test]
#[allow(clippy::too_many_lines)] // one table of info requests against the captured responses
async fn info_responses_have_the_venues_shapes() {
    let (state, user) = world();
    let real = real();
    let cases: Vec<(&str, Value)> = vec![
        ("allPerpMetas", serde_json::json!({"type": "allPerpMetas"})),
        ("meta", serde_json::json!({"type": "meta"})),
        (
            "metaAndAssetCtxs",
            serde_json::json!({"type": "metaAndAssetCtxs"}),
        ),
        (
            "clearinghouseState",
            serde_json::json!({"type": "clearinghouseState", "user": user}),
        ),
        (
            "spotClearinghouseState",
            serde_json::json!({"type": "spotClearinghouseState", "user": user}),
        ),
        (
            "openOrders",
            serde_json::json!({"type": "openOrders", "user": user}),
        ),
        (
            "frontendOpenOrders",
            serde_json::json!({"type": "frontendOpenOrders", "user": user}),
        ),
        (
            "activeAssetData",
            serde_json::json!({"type": "activeAssetData", "user": user, "coin": "ETH"}),
        ),
        (
            "l2Book",
            serde_json::json!({"type": "l2Book", "coin": "ETH"}),
        ),
        (
            "userFills",
            serde_json::json!({"type": "userFills", "user": user}),
        ),
        (
            "userFunding",
            serde_json::json!({"type": "userFunding", "user": user, "startTime": 0}),
        ),
        (
            "preTransferCheck",
            serde_json::json!({"type": "preTransferCheck", "user": user, "source": user}),
        ),
        (
            "userAbstraction",
            serde_json::json!({"type": "userAbstraction", "user": user}),
        ),
        (
            "exchangeStatus",
            serde_json::json!({"type": "exchangeStatus"}),
        ),
        (
            "perpConciseAnnotations",
            serde_json::json!({"type": "perpConciseAnnotations"}),
        ),
        (
            "perpCategories",
            serde_json::json!({"type": "perpCategories"}),
        ),
    ];
    for (name, body) in cases {
        let sim = info_json(&state, body).await;
        let real = &real["info"][name];
        // `allPerpMetas` is one entry per dex: the sim serves the first dex.
        let (real, sim) = if name == "allPerpMetas" {
            (&real[0], &sim[0])
        } else {
            (real, &sim)
        };
        assert!(!sim.is_null(), "{name}: the sim answered null");
        same_shape(real, sim, name);
    }

    // Maps keyed by coin: the values' shape only.
    let mids = info_json(&state, serde_json::json!({"type": "allMids"})).await;
    assert!(mids.as_object().unwrap().values().all(Value::is_string));

    // The first dex only: `perpDexs` lists it as null.
    let dexs = info_json(&state, serde_json::json!({"type": "perpDexs"})).await;
    assert_eq!(dexs, serde_json::json!([null]));
}

/// The venue's order of the two ledgers: fills newest first, funding oldest
/// first.
#[tokio::test]
async fn fills_come_newest_first_and_funding_oldest_first() {
    let (state, user) = world();
    let fills = info_json(
        &state,
        serde_json::json!({"type": "userFills", "user": user}),
    )
    .await;
    let times: Vec<u64> = fills
        .as_array()
        .unwrap()
        .iter()
        .map(|f| f["time"].as_u64().unwrap())
        .collect();
    assert!(!times.is_empty());
    assert!(times.windows(2).all(|w| w[0] >= w[1]), "{times:?}");
    let funding = info_json(
        &state,
        serde_json::json!({"type": "userFunding", "user": user, "startTime": 0}),
    )
    .await;
    assert_eq!(funding[0]["delta"]["type"], "funding");
}

/// The real API's answer to a type it does not know.
#[tokio::test]
async fn an_unknown_info_type_is_the_venues_422() {
    let (state, _) = world();
    let response = info::info(
        State(state),
        Bytes::from(r#"{"type":"fooBar"}"#.to_string()),
    )
    .await
    .unwrap();
    assert_eq!(response.status(), 422);
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    assert_eq!(
        std::str::from_utf8(&bytes).unwrap(),
        "Failed to deserialize the JSON body into the target type"
    );
}

/// Subscribe through the request path (the ack) and take the first pushes.
fn subscribe(state: &ApiState, subscription: &Value) -> (Value, Vec<Value>, Live) {
    let mut subs: Vec<Live> = Vec::new();
    let request = serde_json::json!({"method": "subscribe", "subscription": subscription});
    let ack = request_reply(&request.to_string(), &mut subs, state);
    let ack = serde_json::to_value(&ack).unwrap();
    assert_eq!(ack["channel"], "subscriptionResponse", "{ack}");
    let mut live = subs.pop().expect("subscribed");
    let frames = pushes(&mut live, state, crate::now_ms())
        .iter()
        .map(|f| serde_json::from_str(f).unwrap())
        .collect();
    (ack, frames, live)
}

fn decode(frame: &Value) -> Value {
    // The compressed channels carry base64 of a deflate stream.
    let text = |s: &str| serde_json::from_str::<Value>(&ws_codec::inflate_b64(s)).unwrap();
    match frame["channel"].as_str().unwrap() {
        "fastAssetCtxs" | "pac" => {
            serde_json::json!({"channel": frame["channel"], "data": text(frame["data"].as_str().unwrap())})
        }
        "l2" if frame["data"].get("c").is_some() => serde_json::json!({
            "channel": "l2",
            "data": {"c": text(frame["data"]["c"].as_str().unwrap())},
        }),
        _ => frame.clone(),
    }
}

#[test]
#[allow(clippy::too_many_lines)] // one table of subscriptions against the captured frames
fn websocket_acks_and_frames_have_the_venues_shapes() {
    let (state, user) = world();
    let real = real();
    let subs: Vec<(&str, Value)> = vec![
        ("allMids", serde_json::json!({"type": "allMids"})),
        (
            "activeAssetCtx",
            serde_json::json!({"type": "activeAssetCtx", "coin": "ETH"}),
        ),
        (
            "l2Book",
            serde_json::json!({"type": "l2Book", "coin": "ETH"}),
        ),
        (
            "clearinghouseState",
            serde_json::json!({"type": "clearinghouseState", "user": user}),
        ),
        (
            "openOrders",
            serde_json::json!({"type": "openOrders", "user": user, "dex": ""}),
        ),
        (
            "spotState",
            serde_json::json!({"type": "spotState", "user": user}),
        ),
        (
            "allDexsClearinghouseState",
            serde_json::json!({"type": "allDexsClearinghouseState", "user": user}),
        ),
        (
            "userFills",
            serde_json::json!({"type": "userFills", "user": user}),
        ),
        (
            "userFundings",
            serde_json::json!({"type": "userFundings", "user": user}),
        ),
        (
            "activeAssetData",
            serde_json::json!({"type": "activeAssetData", "user": user, "coin": "ETH"}),
        ),
        (
            "fastAssetCtxs",
            serde_json::json!({"type": "fastAssetCtxs"}),
        ),
        ("pac", serde_json::json!({"type": "pac"})),
        ("l2:l2", serde_json::json!({"type": "l2", "c": "ETH"})),
    ];
    for (name, subscription) in subs {
        let (ack, frames, _) = subscribe(&state, &subscription);
        let real = &real["ws"][name];
        same_shape(&real["ack"], &ack, &format!("{name} ack"));
        let first = decode(
            frames
                .first()
                .unwrap_or_else(|| panic!("{name}: no first frame")),
        );
        if name == "fastAssetCtxs" {
            // A map keyed by coin: the entries' shape, not the coin names.
            let entry = |frame: &Value| {
                frame["data"]
                    .as_object()
                    .unwrap()
                    .values()
                    .next()
                    .unwrap()
                    .clone()
            };
            same_shape(
                &entry(&real["first"]),
                &entry(&first),
                "fastAssetCtxs entry",
            );
            continue;
        }
        same_shape(&real["first"], &first, &format!("{name} first frame"));
    }
}

/// A diffing channel pushes the change and nothing while nothing changes.
#[test]
fn l2_pushes_a_diff_in_the_venues_shape_then_goes_quiet() {
    let (state, _) = world();
    let (_, first, mut live) = subscribe(&state, &serde_json::json!({"type": "l2", "c": "ETH"}));
    assert!(first[0]["data"].get("s").is_some(), "a full book first");
    assert!(
        pushes(&mut live, &state, crate::now_ms()).is_empty(),
        "an unchanged book pushes nothing"
    );
    let mut moved = book("ETH");
    moved.levels[0][0].sz = Decimal::from(77);
    moved.levels[1].remove(0);
    state.feed.put_book(moved, crate::now_ms());
    let frames = pushes(&mut live, &state, crate::now_ms());
    assert_eq!(frames.len(), 1);
    let frame: Value = serde_json::from_str(&frames[0]).unwrap();
    let diff = decode(&frame);
    same_shape(&real()["ws"]["l2:l2"]["later"], &diff, "l2 diff");
}

/// The venue's own subscription errors.
#[test]
fn subscription_errors_read_like_the_venues() {
    let (state, user) = world();
    let mut subs: Vec<Live> = Vec::new();
    let reply = |text: &str, subs: &mut Vec<Live>| {
        serde_json::to_value(request_reply(text, subs, &state)).unwrap()
    };
    let bogus = r#"{"method":"subscribe","subscription":{"type":"bogusChannel"}}"#;
    assert_eq!(
        reply(bogus, &mut subs)["data"],
        format!("Error parsing JSON into valid websocket request: {bogus}")
    );
    let unknown_coin =
        r#"{"method":"subscribe","subscription":{"type":"activeAssetCtx","coin":"NOPE"}}"#;
    assert_eq!(
        reply(unknown_coin, &mut subs)["data"],
        r#"Invalid subscription {"type":"activeAssetCtx","coin":"NOPE"}"#
    );
    let all_mids = r#"{"method":"subscribe","subscription":{"type":"allMids"}}"#;
    assert_eq!(
        reply(all_mids, &mut subs)["channel"],
        "subscriptionResponse"
    );
    assert_eq!(
        reply(all_mids, &mut subs)["data"],
        r#"Already subscribed: {"type":"allMids"}"#
    );
    let _ = user;
}

/// The fill stream: a snapshot first, then only the fills that are new.
#[test]
fn the_fill_stream_pushes_a_snapshot_then_only_new_fills() {
    let (state, user) = world();
    let (_, first, mut live) = subscribe(
        &state,
        &serde_json::json!({"type": "userFills", "user": user}),
    );
    assert_eq!(first[0]["data"]["isSnapshot"], true);
    let snapshot = first[0]["data"]["fills"].as_array().unwrap().len();
    assert!(snapshot >= 1);
    assert!(
        pushes(&mut live, &state, crate::now_ms()).is_empty(),
        "no new fill, no frame"
    );
    state
        .sim
        .order(
            &state.feed,
            user,
            &Order {
                coin: "ETH".into(),
                is_buy: true,
                size: "1".parse().unwrap(),
                limit_px: None,
                tif: Tif::Ioc,
                reduce_only: false,
                cloid: None,
                trigger: None,
            },
        )
        .unwrap();
    let frames = pushes(&mut live, &state, crate::now_ms());
    assert_eq!(frames.len(), 1);
    let frame: Value = serde_json::from_str(&frames[0]).unwrap();
    assert!(
        frame["data"].get("isSnapshot").is_none(),
        "an update, not a snapshot"
    );
    assert_eq!(frame["data"]["fills"].as_array().unwrap().len(), 1);
}

/// The funding stream: the hourly payments as a snapshot, flat rows.
#[test]
fn the_funding_stream_serves_flat_rows() {
    let (state, user) = world();
    let (_, first, mut live) = subscribe(
        &state,
        &serde_json::json!({"type": "userFundings", "user": user}),
    );
    let rows = first[0]["data"]["fundings"].as_array().unwrap();
    assert!(!rows.is_empty(), "the world advanced one funding hour");
    assert!(
        rows[0].get("delta").is_none(),
        "flat, not the info route's wrapper"
    );
    assert!(pushes(&mut live, &state, crate::now_ms()).is_empty());
}
