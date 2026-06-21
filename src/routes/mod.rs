use ginger_shared_rs::rocket_models::MessageResponse;
use rocket::serde::json::Json;
use rocket::State;
use rocket_okapi::openapi;

use crate::wamp_client::SharedWampClient;
pub mod available_devices;

#[openapi()]
#[get("/")]
pub fn index(wamp_client: &State<SharedWampClient>) -> Json<MessageResponse> {
    Json(MessageResponse {
        // channel = "{prefix}_{realm}", e.g. "presence-<instance_id>_ginger-society"
        message: wamp_client.channel().to_string(),
    })
}


use rocket::response::stream::{EventStream, Event};
use rocket::tokio::time::{self, Duration};

#[get("/stream/counter")]
pub async fn stream_counter() -> EventStream![] {
    EventStream! {
        let mut interval = time::interval(Duration::from_secs(1));
        let mut count = 0u64;

        loop {
            interval.tick().await;
            yield Event::data(count.to_string())
                .event("counter")
                .id(count.to_string());
            count += 1;

            if count > 10 {
                break;
            }
        }
    }
}