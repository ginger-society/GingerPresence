use rocket_okapi::JsonSchema;
use serde::{Deserialize, Serialize};


#[derive(Debug, Deserialize, JsonSchema)]
pub struct AvailableDevicesQuery {
    pub capability: String,
}
