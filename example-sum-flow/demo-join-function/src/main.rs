use actix_web::error;
use cloudevent::{Event, EventBuilder, Reader, Writer};
use faas_rust_macro::faas_function;
use maplit::hashmap;
use std::collections::HashMap;

#[faas_function]
pub async fn sum(x: Event, y: Event) -> Result<HashMap<String, Event>, actix_web::Error> {
    let x_number = extract_number_from_event_payload(x)?;
    let y_number = extract_number_from_event_payload(y)?;

    let sum = x_number + y_number;
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
            .ok_or(error::ErrorBadRequest(format!("Event {} payload does not contain a data field", e.id)))?
    )
}
