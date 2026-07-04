// src/routes/available_devices.rs

use redis::AsyncCommands;
use rocket::serde::json::Json;
use rocket::State;
use rocket_okapi::openapi;
use serde::Serialize;

use crate::db::redis::RedisPool;
use crate::handlers::heartbeat::METRICS_PREFIX;

const AVAILABLE_DEVICES_KEY: &str = "available_devices";
const CAPABILITY_SUFFIX: &str = "_capability";

#[derive(Debug, Serialize, schemars::JsonSchema)]
pub struct DeviceCapabilities {
    pub channel: String,
    pub capabilities: Vec<String>,
}

/// Returns every currently-available device and its capabilities.
///
/// Reads the `available_devices` set, then fetches each device's
/// `{channel}_capability` value in a single MGET round trip.
#[openapi()]
#[get("/available-devices")]
pub async fn available_devices(
    redis_pool: &State<RedisPool>,
) -> Json<Vec<DeviceCapabilities>> {
    let mut conn: redis::aio::ConnectionManager = (***redis_pool).clone();

    let channels: Vec<String> = match conn.smembers(AVAILABLE_DEVICES_KEY).await {
        Ok(v) => v,
        Err(e) => {
            eprintln!("[available-devices] smembers failed: {:?}", e);
            return Json(vec![]);
        }
    };

    if channels.is_empty() {
        return Json(vec![]);
    }

    let capability_keys: Vec<String> = channels
        .iter()
        .map(|c| format!("{}{}", c, CAPABILITY_SUFFIX))
        .collect();

    // MGET preserves order — values[i] corresponds to channels[i].
    // A device whose capability key has no value (e.g. it expired between
    // the two calls) gets None and is reported with an empty list rather
    // than dropped, so the response always reflects the membership set.
    let values: Vec<Option<String>> = match conn.mget(&capability_keys).await {
        Ok(v) => v,
        Err(e) => {
            eprintln!("[available-devices] mget failed: {:?}", e);
            return Json(vec![]);
        }
    };

    let result = channels
        .into_iter()
        .zip(values.into_iter())
        .map(|(channel, value)| {
            let capabilities = value
                .and_then(|v| serde_json::from_str::<Vec<String>>(&v).ok())
                .unwrap_or_default();
            DeviceCapabilities {
                channel,
                capabilities,
            }
        })
        .collect();

    Json(result)
}


/// Returns devices that have a specific capability.
/// Returns channels of devices that have a specific capability.
#[openapi()]
#[get("/available-devices/by-capability?<capability>")]
pub async fn available_devices_by_capability(
    redis_pool: &State<RedisPool>,
    capability: String,
) -> Json<Vec<String>> {
    let mut conn: redis::aio::ConnectionManager = (***redis_pool).clone();

    let channels: Vec<String> = match conn.smembers(AVAILABLE_DEVICES_KEY).await {
        Ok(v) => v,
        Err(e) => {
            eprintln!("[available-devices-by-capability] smembers failed: {:?}", e);
            return Json(vec![]);
        }
    };

    if channels.is_empty() {
        return Json(vec![]);
    }

    let capability_keys: Vec<String> = channels
        .iter()
        .map(|c| format!("{}{}", c, CAPABILITY_SUFFIX))
        .collect();

    let values: Vec<Option<String>> = match conn.mget(&capability_keys).await {
        Ok(v) => v,
        Err(e) => {
            eprintln!("[available-devices-by-capability] mget failed: {:?}", e);
            return Json(vec![]);
        }
    };

    let result = channels
        .into_iter()
        .zip(values.into_iter())
        .filter_map(|(channel, value)| {
            let capabilities = value
                .and_then(|v| serde_json::from_str::<Vec<String>>(&v).ok())
                .unwrap_or_default();

            if capabilities.contains(&capability) {
                Some(channel)
            } else {
                None
            }
        })
        .collect();

    Json(result)
}


// src/routes/device_metrics.rs
//
// Prometheus-compatible scrape endpoint for a single device.
//
// Point a Prometheus `scrape_config` at this per device, e.g.:
//
//   scrape_configs:
//     - job_name: 'ginger-devices'
//       metrics_path: /device-metrics/presence-abc_rackmint
//       static_configs:
//         - targets: ['your-host:your-port']
//
// The response body is exactly what the device sent in `metrics_text` on
// its last heartbeat (see handlers/heartbeat.rs), forwarded byte-for-byte
// with the standard Prometheus exposition content type. Nothing here
// re-parses or re-encodes the blob — it was already valid Prometheus text
// when the device produced it.

use rocket::http::{Header, Status};
use rocket::response::{self, Responder, Response};
use std::io::Cursor;



/// Wraps a raw Prometheus text body so it's served with the exact content
/// type Prometheus scrapers expect (`text/plain; version=0.0.4;
/// charset=utf-8`), rather than Rocket's default text/plain.
pub struct PrometheusText(String);

impl<'r> Responder<'r, 'static> for PrometheusText {
    fn respond_to(self, _req: &rocket::Request) -> response::Result<'static> {
        Response::build()
            .header(Header::new(
                "Content-Type",
                "text/plain; version=0.0.4; charset=utf-8",
            ))
            .sized_body(self.0.len(), Cursor::new(self.0))
            .ok()
    }
}

/// Serves the most recently stored metrics blob for `channel_id`.
///
/// 404 if the device has never sent metrics, doesn't have the "metrics"
/// capability enabled, or its `metrics_*` key has expired (device is gone
/// or hasn't heartbeated with metrics recently enough) — Prometheus will
/// correctly mark the scrape target "down" rather than seeing a false
/// empty-but-healthy response.
///
/// Not wrapped in #[openapi()]: a raw Prometheus text body isn't a
/// JSON-schema-able response, so this belongs in the plain `routes![...]`
/// list rather than `openapi_get_routes![...]`.
#[get("/device-metrics/<channel_id>")]
pub async fn device_metrics(
    redis_pool: &State<RedisPool>,
    channel_id: String,
) -> Result<PrometheusText, Status> {
    let mut conn: redis::aio::ConnectionManager = (***redis_pool).clone();

    let metrics_key = format!("{}{}", METRICS_PREFIX, channel_id);

    let metrics_text: Option<String> = conn.get(&metrics_key).await.map_err(|e| {
        eprintln!("[device-metrics] get failed for '{}': {:?}", channel_id, e);
        Status::InternalServerError
    })?;

    metrics_text.map(PrometheusText).ok_or(Status::NotFound)
}