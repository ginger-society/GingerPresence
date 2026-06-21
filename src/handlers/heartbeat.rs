// src/handlers/heartbeat.rs
//
// WAMP RPC handler `handle_heartbeat` + the Redis-backed presence tracker
// it relies on.
//
// Redis layout:
//   {device_channel}_capability   -> JSON array of capability strings
//   {device_channel}_last_seen    -> timestamp string, TTL = HEARTBEAT_TTL_SECS
//   available_devices             -> Redis SET of device_channel strings
//
// When a `{device_channel}_last_seen` key expires, Redis emits an `expired`
// keyevent (requires `notify-keyspace-events` to include `Ex` — we set this
// ourselves in `db::redis::create_redis_pool`, same as the notification
// broker does). `start_expiry_watcher` subscribes to that and removes the
// device from `available_devices` plus its `_capability` key.

use futures::StreamExt;
use redis::AsyncCommands;
use serde_json::Value;

use crate::db::redis::RedisPool;
use crate::wamp_client::SharedWampClient;

/// TTL (seconds) for the `_last_seen` presence key. Devices must heartbeat
/// more often than this or they're considered gone.
const HEARTBEAT_TTL_SECS: u64 = 15;

const AVAILABLE_DEVICES_KEY: &str = "available_devices";
const LAST_SEEN_SUFFIX: &str = "_last_seen";
const CAPABILITY_SUFFIX: &str = "_capability";

/// Register the `handle_heartbeat` RPC on the given WAMP client.
///
/// Expected kwargs payload:
/// ```json
/// {
///   "device_channel": "presence-abc_rackmint",
///   "capabilities": ["gpu", "camera"]
/// }
/// ```
pub async fn register_heartbeat_handler(wamp_client: &SharedWampClient, redis_pool: RedisPool) {
    wamp_client
        .register("handle_heartbeat", move |_args, kwargs| {
            let redis_pool = redis_pool.clone();
            async move { handle_heartbeat(kwargs, redis_pool).await }
        })
        .await;
}

async fn handle_heartbeat(kwargs: Option<Value>, redis_pool: RedisPool) -> Result<Value, Value> {
    let kwargs = kwargs.ok_or_else(|| serde_json::json!({"error": "missing kwargs"}))?;

    let device_channel = kwargs
        .get("device_channel")
        .and_then(|v| v.as_str())
        .ok_or_else(|| serde_json::json!({"error": "missing 'device_channel'"}))?
        .to_string();

    let capabilities: Vec<String> = kwargs
        .get("capabilities")
        .and_then(|v| v.as_array())
        .ok_or_else(|| serde_json::json!({"error": "missing 'capabilities' (expected array)"}))?
        .iter()
        .filter_map(|v| v.as_str().map(|s| s.to_string()))
        .collect();

    let capability_key = format!("{}{}", device_channel, CAPABILITY_SUFFIX);
    let last_seen_key = format!("{}{}", device_channel, LAST_SEEN_SUFFIX);
    let capabilities_json = serde_json::to_string(&capabilities)
        .map_err(|e| serde_json::json!({"error": e.to_string()}))?;
    let now = chrono::Utc::now().to_rfc3339();

    let mut conn = (*redis_pool).clone();

    // add/update capability key (no expiry — only last_seen expires)
    conn.set::<_, _, ()>(&capability_key, &capabilities_json)
        .await
        .map_err(|e| serde_json::json!({"error": e.to_string()}))?;

    // last_seen, with TTL — this is the key whose expiry drives cleanup
    conn.set_ex::<_, _, ()>(&last_seen_key, &now, HEARTBEAT_TTL_SECS)
        .await
        .map_err(|e| serde_json::json!({"error": e.to_string()}))?;

    // track in the global available_devices set
    conn.sadd::<_, _, ()>(AVAILABLE_DEVICES_KEY, &device_channel)
        .await
        .map_err(|e| serde_json::json!({"error": e.to_string()}))?;

    Ok(serde_json::json!({"status": "ok"}))
}

/// Spawns a background task that listens for Redis key-expiry events and
/// reconciles `available_devices` / `_capability` when a device's
/// `_last_seen` key expires.
///
/// Requires the Redis server to have `notify-keyspace-events` including
/// `Ex` — `db::redis::create_redis_pool` sets this on connect, same as the
/// notification broker's `connect_redis`.
pub fn start_expiry_watcher(redis_url: String, redis_pool: RedisPool) {
    tokio::spawn(async move {
        loop {
            let client = match redis::Client::open(redis_url.clone()) {
                Ok(c) => c,
                Err(e) => {
                    eprintln!("[heartbeat-expiry] client error: {:?} — retrying in 2s", e);
                    tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
                    continue;
                }
            };

            let mut pubsub = match client.get_async_pubsub().await {
                Ok(p) => p,
                Err(e) => {
                    eprintln!("[heartbeat-expiry] connect failed: {:?} — retrying in 2s", e);
                    tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
                    continue;
                }
            };

            // db 0 is the default — adjust the index here if REDIS_URI points
            // at a different logical db.
            if let Err(e) = pubsub.psubscribe("__keyevent@0__:expired").await {
                eprintln!("[heartbeat-expiry] psubscribe failed: {:?} — retrying in 2s", e);
                tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
                continue;
            }

            println!("[heartbeat-expiry] watcher active");

            let mut stream = pubsub.into_on_message();

            while let Some(msg) = stream.next().await {
                let expired_key: String = match msg.get_payload() {
                    Ok(k) => k,
                    Err(_) => continue,
                };

                let Some(device_channel) = expired_key.strip_suffix(LAST_SEEN_SUFFIX) else {
                    continue;
                };

                // acquire lock — only one presence instance should do the cleanup
                let lock_key = format!("cleanup_lock:{}", device_channel);
                let mut conn = (*redis_pool).clone();
                let acquired: bool = redis::cmd("SET")
                    .arg(&lock_key)
                    .arg("1")
                    .arg("NX")
                    .arg("EX")
                    .arg(10u64)
                    .query_async(&mut conn)
                    .await
                    .unwrap_or(false);

                if !acquired {
                    println!(
                        "[heartbeat-expiry] cleanup already claimed for '{}' — skipping",
                        device_channel
                    );
                    continue;
                }

                if let Err(e) = cleanup_device(&redis_pool, device_channel).await {
                    eprintln!(
                        "[heartbeat-expiry] failed to clean up expired device '{}': {:?}",
                        device_channel, e
                    );
                } else {
                    println!("[heartbeat-expiry] device '{}' expired — cleaned up", device_channel);
                }
            }

            eprintln!("[heartbeat-expiry] stream ended — reconnecting in 2s...");
            tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
        }
    });
}

async fn cleanup_device(redis_pool: &RedisPool, device_channel: &str) -> redis::RedisResult<()> {
    let capability_key = format!("{}{}", device_channel, CAPABILITY_SUFFIX);
    let mut conn = (**redis_pool).clone();

    conn.srem::<_, _, ()>(AVAILABLE_DEVICES_KEY, device_channel).await?;
    conn.del::<_, ()>(&capability_key).await?;

    Ok(())
}