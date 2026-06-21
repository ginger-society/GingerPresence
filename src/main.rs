#[macro_use]
extern crate rocket;

use dotenv::dotenv;
use rocket::{Build, Rocket};
use rocket_okapi::openapi_get_routes;
use rocket_okapi::swagger_ui::{make_swagger_ui, SwaggerUIConfig};
use rocket_prometheus::PrometheusMetrics;
use std::env;
use std::sync::Arc;
use uuid::Uuid; // ← add uuid = "1" to Cargo.toml if not already present

use db::redis::create_redis_pool;

mod db;
mod fairings;
mod handlers;
mod middlewares;
mod models;
mod routes;
mod wamp_client;

use crate::handlers::heartbeat::{register_heartbeat_handler, start_expiry_watcher};
use crate::wamp_client::{SharedWampClient, WampClient};

const SERVICE_PREFIX: &str = "presence";

#[tokio::main]
async fn main() {
    dotenv().ok();

    println!("Starting server...");

    let instance_id = Uuid::new_v4().to_string();
    let wamp_prefix = format!("{}-{}", SERVICE_PREFIX, instance_id);
    println!("WAMP prefix: {}", wamp_prefix);

    let wamp_client: SharedWampClient = Arc::new(WampClient::new(
        &wamp_prefix,
        &env::var("ISC_SECRET").unwrap(),
        "ginger-society",
    ));

    let wamp_listen_client = wamp_client.clone();
    tokio::spawn(async move {
        wamp_listen_client.listen().await;
    });

    let prometheus = PrometheusMetrics::new();

    let mut server = rocket::build()
        .attach(fairings::cors::CORS)
        .attach(prometheus.clone())
        .mount(
            format!("/{}/", SERVICE_PREFIX),
            openapi_get_routes![routes::index, 
            routes::available_devices::available_devices,
            routes::available_devices::available_devices_by_capability,
            ],
        )
        .mount(
            format!("/{}/api-docs", SERVICE_PREFIX),
            make_swagger_ui(&SwaggerUIConfig {
                url: "../openapi.json".to_owned(),
                ..Default::default()
            }),
        )
        .mount(format!("/{}/metrics", SERVICE_PREFIX), prometheus)
        .mount("/", routes![
            routes::stream_counter,   // SSE routes go here, outside openapi
        ]);

    server = server.manage(wamp_client.clone());

    match env::var("MONGO_URI") {
        Ok(mongo_uri) => match env::var("MONGO_DB_NAME") {
            Ok(mongo_db_name) => {
                println!("Attempting to connect to mongo");
                server = server.manage(db::connect_mongo(mongo_uri, mongo_db_name))
            }
            Err(_) => {
                println!("Not connecting to mongo, missing MONGO_DB_NAME")
            }
        },
        Err(_) => println!("Not connecting to mongo, missing MONGO_URI"),
    };

    match env::var("REDIS_URI") {
        Ok(redis_uri) => {
            println!("Attempting to connect to redis");
            let redis_pool = create_redis_pool(redis_uri.clone()).await;

            // register the heartbeat RPC handler — uses its own clone of the pool
            register_heartbeat_handler(&wamp_client, redis_pool.clone()).await;

            // background task: reconciles available_devices/_capability when
            // a device's _last_seen key expires. Opens its own dedicated
            // redis::Client + pub/sub connection (separate from the
            // ConnectionManager pool used for normal commands) — same split
            // the notification broker uses between connect_redis and its
            // pubsub bridge.
            start_expiry_watcher(redis_uri, redis_pool.clone());

            server = server.manage(redis_pool);
        }
        Err(_) => println!("Not connecting to redis"),
    }

    server.launch().await.expect("Failed to launch Rocket");
}

// Unit testings
#[cfg(test)]
mod tests;