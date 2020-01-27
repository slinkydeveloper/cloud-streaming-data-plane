use std::collections::HashMap;

use actix_web::error;
use cloudevent::{Event, EventBuilder, Reader, Writer};
use faas_rust_macro::faas_function;
use maplit::hashmap;

#[faas_function]
pub async fn sum(inbound_a: Event, inbound_b: Event) -> Result<HashMap<String, Event>, actix_web::Error> {
    let inbound_a_number = extract_number_from_event_payload(inbound_a)?;
    let inbound_b_number = extract_number_from_event_payload(inbound_b)?;

    let sum = inbound_a_number + inbound_b_number;
    let output_stream = if sum >= 0f64 {
        "positive"
    } else {
        "negative"
    };

    let mut event: Event = EventBuilder::default()
        .event_type(format!("{}.demo", output_stream))
        .build()
        .map_err(|e| actix_web::error::ErrorInternalServerError(e))?;
    event.write_payload("application/json", serde_json::json!({
        "data": sum
    }))?;

    Ok(hashmap! {
        output_stream.to_string() => event
    })
}

fn extract_number_from_event_payload(e: Event) -> Result<f64, actix_web::Error> {
    let json_payload: serde_json::Value = e.read_payload()
        .ok_or(error::ErrorBadRequest(format!("Event {} is missing a payload", e.id)))??;
    Ok(
        json_payload
            .pointer("/data")
            .and_then(|o| o.as_f64())
            .ok_or(error::ErrorBadRequest(format!("Event {} payload does not contain a value field", e.id)))?
    )
}
