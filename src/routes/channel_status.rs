// src/routes/channel_status.rs

use redis::AsyncCommands;
use rocket::serde::json::Json;
use rocket::State;
use rocket_okapi::openapi;
use serde::Serialize;

use crate::db::redis::RedisPool;

const LAST_SEEN_SUFFIX: &str = "_last_seen";

#[derive(Debug, Serialize, schemars::JsonSchema)]
pub struct ChannelStatusResponse {
    pub statuses: Vec<bool>,
}

/// Takes a comma-separated list of channel IDs and returns their online status
/// in the same order. A channel is considered online if its `_last_seen` key
/// exists in Redis (i.e. has not expired).
///
/// Example: `GET /channel-status?channels=channel_a,channel_b,channel_c`
/// Returns: `{"statuses": [true, false, true]}`
#[openapi()]
#[get("/channel-status?<channels>")]
pub async fn channel_status(
    redis_pool: &State<RedisPool>,
    channels: String,
) -> Json<ChannelStatusResponse> {
    let channel_list: Vec<String> = channels
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect();

    if channel_list.is_empty() {
        return Json(ChannelStatusResponse { statuses: vec![] });
    }

    let last_seen_keys: Vec<String> = channel_list
        .iter()
        .map(|c| format!("{}{}", c, LAST_SEEN_SUFFIX))
        .collect();

    let mut conn: redis::aio::ConnectionManager = (***redis_pool).clone();

    // EXISTS with multiple keys returns the count of keys that exist, not a
    // per-key boolean. We use MGET instead — None means the key doesn't exist
    // (expired or never set), Some(_) means it's alive.
    let values: Vec<Option<String>> = match conn.mget(&last_seen_keys).await {
        Ok(v) => v,
        Err(e) => {
            eprintln!("[channel-status] mget failed: {:?}", e);
            // return all false on redis error rather than 500
            return Json(ChannelStatusResponse {
                statuses: vec![false; channel_list.len()],
            });
        }
    };

    let statuses = values.into_iter().map(|v| v.is_some()).collect();

    Json(ChannelStatusResponse { statuses })
}