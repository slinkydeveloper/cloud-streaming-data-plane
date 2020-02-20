use std::collections::HashMap;
use std::env;

use actix_web::error;
use cloudevent::{Event, EventBuilder, Reader, Writer};
use faas_rust_macro::faas_function;
use maplit::hashmap;

use lazy_static::lazy_static;

lazy_static! {
    static ref STOCKS: Vec<String> = env::var("STOCKS")
        .unwrap()
        .split(",")
        .map(|s| s.to_string())
        .collect();
}

#[faas_function]
pub async fn filter_stocks(input: Event) -> Result<HashMap<String, Event>, actix_web::Error> {
    let input_json_payload: serde_json::Value = input.read_payload()
        .ok_or(error::ErrorBadRequest(format!("Event {} is missing a payload", input.id)))??;

    let stock_name = input_json_payload
        .pointer("/name")
        .and_then(|o| o.as_str())
        .ok_or(error::ErrorBadRequest(format!("Event {} payload does not contain a name field", input.id)))?
        .to_string();

    if STOCKS.contains(&stock_name) {
        let mut event: Event = EventBuilder::default()
            .event_type("stock-update.stocks-demo")
            .id(input.id)
            .extensions(hashmap! {
                "stockname".to_string() => stock_name
            })
            .build()
            .map_err(actix_web::error::ErrorInternalServerError)?;


        event.write_payload("application/json", input_json_payload)?;

        Ok(hashmap! {
            "output".to_string() => event
        })
    } else {
        Ok(hashmap! {})
    }
}
