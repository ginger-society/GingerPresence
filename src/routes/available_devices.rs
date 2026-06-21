// src/routes/available_devices.rs

use redis::AsyncCommands;
use rocket::serde::json::Json;
use rocket::State;
use rocket_okapi::openapi;
use serde::Serialize;

use crate::db::redis::RedisPool;

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