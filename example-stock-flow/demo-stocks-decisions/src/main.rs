use std::collections::HashMap;
use std::convert::TryInto;
use std::env;

use actix_web::error;
use cloudevent::{Event, EventBuilder, Reader, Writer};
use faas_rust_macro::faas_function;
use maplit::hashmap;
use serde_json::json;

use lazy_static::lazy_static;
use state::StockState;
use state::StockUpdate;

mod state;

lazy_static! {
    static ref BUY_THRESHOLD: f64 = env::var("BUY_THRESHOLD")
        .unwrap()
        .parse()
        .unwrap();

    static ref SELL_THRESHOLD: f64 = env::var("SELL_THRESHOLD")
        .unwrap()
        .parse()
        .unwrap();
}

#[faas_function]
pub async fn stock_decision(input: Event, state: Option<Event>) -> Result<HashMap<String, Event>, actix_web::Error> {
    let input_json_payload: serde_json::Value = input.read_payload()
        .ok_or(error::ErrorBadRequest(format!("input Event {} is missing a payload", input.id)))??;

    let stock_name = input_json_payload
        .pointer("/name")
        .and_then(|o| o.as_str())
        .ok_or(error::ErrorBadRequest(format!("Event {} payload does not contain a name field", input.id)))?
        .to_string();

    let stock_last_value = input_json_payload
        .pointer("/value")
        .and_then(|o| o.as_f64())
        .ok_or(error::ErrorBadRequest(format!("Event {} payload does not contain a value field", input.id)))?;

    println!("Received stock update {}: {}", stock_name, stock_last_value);

    let state = match state {
        None => StockState::new(stock_name.clone()),
        Some(e) => e.try_into()?
    };

    println!("Previous state {:#?}", state);

    let (update_result, new_state) = state.update(stock_last_value, &SELL_THRESHOLD, &BUY_THRESHOLD);

    println!("Updated state {:#?}", new_state);
    println!("Update result {:#?}", update_result);

    println!("{:#?}", input_json_payload);

    return match update_result {
        StockUpdate::None =>
            Ok(hashmap! {
                "state".to_string() => new_state.try_into()?
            }),
        StockUpdate::Buy =>
            Ok(hashmap! {
                "state".to_string() => new_state.try_into()?,
                "output".to_string() => generate_decision_event(stock_name, "buy".to_string())?
            }),
        StockUpdate::Sell =>
            Ok(hashmap! {
                "state".to_string() => new_state.try_into()?,
                "output".to_string() => generate_decision_event(stock_name, "sell".to_string())?
            })
    }
}

fn generate_decision_event(stock_name: String, decision: String) -> Result<Event, actix_web::Error> {
    let mut event = EventBuilder::default()
        .event_type("stock-decision.stocks-demo")
        .extensions(hashmap! {
                "stockname".to_string() => stock_name.clone()
        })
        .build()
        .map_err(actix_web::error::ErrorInternalServerError)?;

    event.write_payload(
        "application/json",
        json!({
            "name": stock_name,
            "decision": decision
        }),
    )?;

    Ok(event)
}
