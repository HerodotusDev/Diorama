//! The frame codecs of the venue's fast websocket channels, matching what the
//! real venue pushes (captured live): `fastAssetCtxs` and `pac` carry base64 of
//! a raw deflate stream of JSON; `l2` carries a full book first and compressed
//! upsert/removal diffs after it.
//!
//! The functions here are pure (state in, JSON out) so the session loop in
//! `ws.rs` only decides *when* to push.
use crate::feed::MarketFeed;
use crate::wire::{decimal_nano, AssetCtx, L2Book, Level};
use base64::Engine;
use flate2::write::DeflateEncoder;
use flate2::Compression;
use rust_decimal::Decimal;
use serde_json::{json, Map, Value};
use std::collections::BTreeMap;
use std::io::Write;

/// Base64 (standard alphabet) of the raw deflate stream of `json`: the
/// encoding the venue's fast channels use (the frontend's `inflate`).
pub(crate) fn deflate_b64(json: &str) -> String {
    let mut encoder = DeflateEncoder::new(Vec::new(), Compression::fast());
    // Writing to a Vec cannot fail.
    let _ = encoder.write_all(json.as_bytes());
    let bytes = encoder.finish().unwrap_or_default();
    base64::engine::general_purpose::STANDARD.encode(bytes)
}

fn text(value: &Decimal) -> Value {
    Value::String(decimal_nano::text(value))
}

// ---------------------------------------------------------------- fastAssetCtxs

/// One coin's fast context: the mark and the mid (null when the book is
/// one-sided).
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct FastCtx {
    pub mark: Decimal,
    pub mid: Option<Decimal>,
}

/// Every served perp's mark and mid: the `fastAssetCtxs` state.
pub(crate) fn fast_snapshot(feed: &MarketFeed) -> BTreeMap<String, FastCtx> {
    let mut out = BTreeMap::new();
    let Some(meta) = feed.meta() else {
        return out;
    };
    let mids = feed.all_mids().0;
    for asset in &meta.universe {
        let Some(mark) = feed.mark(&asset.name) else {
            continue;
        };
        let mid = crate::sim::account::ctx_of(feed, &asset.name)
            .and_then(|ctx| ctx.mid_px)
            .or_else(|| mids.get(&asset.name).copied());
        out.insert(asset.name.clone(), FastCtx { mark, mid });
    }
    out
}

/// The frame body: the whole map on the first push, only the changed fields of
/// the changed coins after it (`None` when nothing changed: no frame).
pub(crate) fn fast_frame(
    prev: Option<&BTreeMap<String, FastCtx>>,
    next: &BTreeMap<String, FastCtx>,
) -> Option<String> {
    let mut out = Map::new();
    for (coin, now) in next {
        let before = prev.and_then(|p| p.get(coin));
        let mut fields = Map::new();
        if before.is_none_or(|b| b.mark != now.mark) {
            fields.insert("markPx".into(), text(&now.mark));
        }
        if before.is_none_or(|b| b.mid != now.mid) {
            fields.insert("midPx".into(), now.mid.as_ref().map_or(Value::Null, text));
        }
        if !fields.is_empty() {
            out.insert(coin.clone(), Value::Object(fields));
        }
    }
    if out.is_empty() && prev.is_some() {
        return None;
    }
    Some(deflate_b64(&Value::Object(out).to_string()))
}

// ---------------------------------------------------------------- pac

/// The `pac` fields of one perp, in the venue's own names.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct PacCtx {
    pub funding: Decimal,
    pub open_interest: Decimal,
    pub prev_day_px: Decimal,
    pub day_ntl_vlm: Decimal,
    pub oracle_px: Decimal,
    pub mark_px: Decimal,
    pub mid_px: Option<Decimal>,
    pub day_base_vlm: Decimal,
}

impl PacCtx {
    fn of(ctx: &AssetCtx) -> Self {
        Self {
            funding: ctx.funding,
            open_interest: ctx.open_interest,
            prev_day_px: ctx.prev_day_px,
            day_ntl_vlm: ctx.day_ntl_vlm,
            oracle_px: ctx.oracle_px,
            mark_px: ctx.mark_px,
            mid_px: ctx.mid_px,
            day_base_vlm: ctx.day_base_vlm,
        }
    }

    /// The fields that differ from `before` (all of them with no `before`).
    fn patch(&self, before: Option<&Self>) -> Map<String, Value> {
        let mut out = Map::new();
        let mut put = |key: &str, changed: bool, value: Value| {
            if changed {
                out.insert(key.into(), value);
            }
        };
        put(
            "funding",
            before.is_none_or(|b| b.funding != self.funding),
            text(&self.funding),
        );
        put(
            "openInterest",
            before.is_none_or(|b| b.open_interest != self.open_interest),
            text(&self.open_interest),
        );
        put(
            "prevDayPx",
            before.is_none_or(|b| b.prev_day_px != self.prev_day_px),
            text(&self.prev_day_px),
        );
        put(
            "dayNtlVlm",
            before.is_none_or(|b| b.day_ntl_vlm != self.day_ntl_vlm),
            text(&self.day_ntl_vlm),
        );
        put(
            "oraclePx",
            before.is_none_or(|b| b.oracle_px != self.oracle_px),
            text(&self.oracle_px),
        );
        put(
            "markPx",
            before.is_none_or(|b| b.mark_px != self.mark_px),
            text(&self.mark_px),
        );
        put(
            "midPx",
            before.is_none_or(|b| b.mid_px != self.mid_px),
            self.mid_px.as_ref().map_or(Value::Null, text),
        );
        put(
            "dayBaseVlm",
            before.is_none_or(|b| b.day_base_vlm != self.day_base_vlm),
            text(&self.day_base_vlm),
        );
        out
    }
}

/// The first dex's contexts in universe order (`None` for a coin with no live
/// context: its slot stays empty so the indexes keep lining up).
pub(crate) fn pac_snapshot(feed: &MarketFeed) -> Option<Vec<Option<PacCtx>>> {
    let meta = feed.meta()?;
    Some(
        meta.universe
            .iter()
            .map(|asset| crate::sim::account::ctx_of(feed, &asset.name).map(|ctx| PacCtx::of(&ctx)))
            .collect(),
    )
}

/// The frame body (`[[dex, [ctx, ...]]]`): every field on the first push, only
/// the changed fields after it (an unchanged coin is `{}` so the indexes keep
/// lining up). `None` when nothing changed.
pub(crate) fn pac_frame(
    prev: Option<&Vec<Option<PacCtx>>>,
    next: &[Option<PacCtx>],
) -> Option<String> {
    let mut any = prev.is_none();
    let slots: Vec<Value> = next
        .iter()
        .enumerate()
        .map(|(i, now)| {
            let before = prev.and_then(|p| p.get(i)).and_then(Option::as_ref);
            let patch = now.as_ref().map(|n| n.patch(before)).unwrap_or_default();
            any |= !patch.is_empty();
            Value::Object(patch)
        })
        .collect();
    any.then(|| deflate_b64(&json!([["", slots]]).to_string()))
}

// ---------------------------------------------------------------- l2

fn side_levels(levels: &[Level]) -> BTreeMap<Decimal, Decimal> {
    levels.iter().map(|l| (l.px, l.sz)).collect()
}

/// The upsert/removal diff between two books of one coin, in the venue's `l2`
/// vocabulary: `l[side]` the levels whose size is new or changed, `r[side]`
/// the indexes (descending) of the previous levels that are gone. `None` when
/// the books are the same.
pub(crate) fn l2_diff(prev: &L2Book, next: &L2Book) -> Option<String> {
    let mut upserts: Vec<Vec<Value>> = vec![Vec::new(), Vec::new()];
    let mut removed: Vec<Vec<usize>> = vec![Vec::new(), Vec::new()];
    for side in 0..2 {
        let before = side_levels(&prev.levels[side]);
        let after = side_levels(&next.levels[side]);
        for level in &next.levels[side] {
            if before.get(&level.px) != Some(&level.sz) {
                upserts[side].push(
                    json!({"p": decimal_nano::text(&level.px), "s": decimal_nano::text(&level.sz)}),
                );
            }
        }
        for (i, level) in prev.levels[side].iter().enumerate().rev() {
            if !after.contains_key(&level.px) {
                removed[side].push(i);
            }
        }
    }
    if upserts.iter().all(Vec::is_empty) && removed.iter().all(Vec::is_empty) {
        return None;
    }
    let body = json!({"c": next.coin, "t": next.time, "l": upserts, "r": removed});
    Some(body.to_string())
}

#[cfg(test)]
pub(crate) fn inflate_b64(b64: &str) -> String {
    use flate2::read::DeflateDecoder;
    use std::io::Read;
    let bytes = base64::engine::general_purpose::STANDARD
        .decode(b64)
        .expect("base64");
    let mut out = String::new();
    DeflateDecoder::new(bytes.as_slice())
        .read_to_string(&mut out)
        .expect("inflate");
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn level(px: &str, sz: &str) -> Level {
        Level {
            px: px.parse().unwrap(),
            sz: sz.parse().unwrap(),
            n: 1,
        }
    }

    fn book(bids: Vec<Level>, asks: Vec<Level>, time: u64) -> L2Book {
        L2Book {
            coin: "BTC".into(),
            time,
            levels: [bids, asks],
            spread: None,
        }
    }

    #[test]
    fn deflate_round_trips_through_the_frontends_inflate() {
        let json = r#"{"BTC":{"markPx":"1","midPx":null}}"#;
        assert_eq!(inflate_b64(&deflate_b64(json)), json);
    }

    #[test]
    fn fast_frames_are_full_first_then_changed_fields_only() {
        let a = BTreeMap::from([(
            "BTC".to_string(),
            FastCtx {
                mark: "100".parse().unwrap(),
                mid: Some("100.5".parse().unwrap()),
            },
        )]);
        let first: Value =
            serde_json::from_str(&inflate_b64(&fast_frame(None, &a).unwrap())).unwrap();
        assert_eq!(first, json!({"BTC": {"markPx": "100", "midPx": "100.5"}}));

        let mut b = a.clone();
        b.get_mut("BTC").unwrap().mid = None;
        let patch: Value =
            serde_json::from_str(&inflate_b64(&fast_frame(Some(&a), &b).unwrap())).unwrap();
        assert_eq!(
            patch,
            json!({"BTC": {"midPx": null}}),
            "only the changed field"
        );
        assert!(
            fast_frame(Some(&b), &b).is_none(),
            "nothing changed, no frame"
        );
    }

    #[test]
    fn pac_frames_keep_one_slot_per_coin() {
        let ctx = |mark: &str| PacCtx {
            funding: "0.0000125".parse().unwrap(),
            open_interest: "10".parse().unwrap(),
            prev_day_px: "90".parse().unwrap(),
            day_ntl_vlm: "1000".parse().unwrap(),
            oracle_px: "100".parse().unwrap(),
            mark_px: mark.parse().unwrap(),
            mid_px: None,
            day_base_vlm: "5".parse().unwrap(),
        };
        let first = vec![Some(ctx("100")), Some(ctx("200"))];
        let v: Value =
            serde_json::from_str(&inflate_b64(&pac_frame(None, &first).unwrap())).unwrap();
        let slots = v[0][1].as_array().unwrap();
        assert_eq!(v[0][0], "");
        assert_eq!(slots.len(), 2);
        assert_eq!(slots[0].as_object().unwrap().len(), 8, "every field first");
        assert_eq!(slots[0]["midPx"], Value::Null);

        let next = vec![Some(ctx("100")), Some(ctx("201"))];
        let v: Value =
            serde_json::from_str(&inflate_b64(&pac_frame(Some(&first), &next).unwrap())).unwrap();
        let slots = v[0][1].as_array().unwrap();
        assert_eq!(
            slots[0],
            json!({}),
            "an unchanged coin keeps its empty slot"
        );
        assert_eq!(slots[1], json!({"markPx": "201"}));
        assert!(pac_frame(Some(&next), &next).is_none());
    }

    #[test]
    fn l2_diff_lists_upserts_and_removed_previous_indexes() {
        let prev = book(
            vec![level("100", "1"), level("99", "2"), level("98", "3")],
            vec![level("101", "1")],
            1,
        );
        let next = book(
            vec![level("100", "1"), level("98", "4"), level("97", "5")],
            vec![level("101", "1")],
            2,
        );
        let diff: Value = serde_json::from_str(&l2_diff(&prev, &next).unwrap()).unwrap();
        assert_eq!(diff["c"], "BTC");
        assert_eq!(diff["t"], 2);
        assert_eq!(
            diff["l"],
            json!([[{"p": "98", "s": "4"}, {"p": "97", "s": "5"}], []])
        );
        assert_eq!(diff["r"], json!([[1], []]), "index 1 (99) is gone");
        assert!(l2_diff(&next, &next).is_none());
    }
}
