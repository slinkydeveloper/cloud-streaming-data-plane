use actix_web::error;
use cloudevent::{Event, Reader};
use faas_rust_macro::faas_function;

#[faas_function]
pub async fn log_stock_decision(input: Event) -> Result<Option<Event>, actix_web::Error> {
    let input_json_payload: serde_json::Value = input.read_payload()
        .ok_or(error::ErrorBadRequest(format!("Event {} is missing a payload", input.id)))??;

    println!("{:#?}", input_json_payload);

    Ok(None)
}
