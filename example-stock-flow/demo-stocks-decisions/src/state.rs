use std::convert::TryFrom;

use actix_web::error;
use cloudevent::{Event, EventBuilder, Reader, Writer};
use maplit::hashmap;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
pub struct StockState {
    name: String,
    owned: bool,
    last_value: Option<f64>,
    coefficient: f64,
}

#[derive(Debug)]
pub enum StockUpdate {
    Buy,
    Sell,
    None,
}

impl StockState {
    pub fn new(name: String) -> StockState {
        StockState {
            name,
            owned: false,
            last_value: None,
            coefficient: 0.0,
        }
    }

    pub fn update(&self, new_value: f64, sell_threshold: &f64, buy_threshold: &f64) -> (StockUpdate, StockState) {
        match self.last_value {
            None => (StockUpdate::None, StockState {
                name: self.name.clone(),
                owned: self.owned,
                last_value: Some(new_value),
                coefficient: self.coefficient,
            }),
            Some(old_value) => {
                let new_coefficient = ((new_value / old_value) - 1f64) + self.coefficient;
                println!("({} / {}) - 1 + {} = {}", new_value, old_value, self.coefficient, new_coefficient);
                if new_coefficient > 0f64 && new_coefficient >= *buy_threshold && !self.owned {
                    (StockUpdate::Buy, StockState {
                        name: self.name.clone(),
                        owned: true,
                        last_value: Some(new_value),
                        coefficient: new_coefficient,
                    })
                } else if new_coefficient < 0f64 && new_coefficient <= *sell_threshold && self.owned {
                    (StockUpdate::Sell, StockState {
                        name: self.name.clone(),
                        owned: false,
                        last_value: Some(new_value),
                        coefficient: new_coefficient,
                    })
                } else {
                    (StockUpdate::None, StockState {
                        name: self.name.clone(),
                        owned: self.owned,
                        last_value: Some(new_value),
                        coefficient: new_coefficient,
                    })
                }
            }
        }
    }
}

impl TryFrom<Event> for StockState {
    type Error = actix_web::Error;

    fn try_from(value: Event) -> Result<Self, Self::Error> {
        Ok(
            serde_json::from_value(
                value
                    .read_payload()
                    .ok_or(error::ErrorBadRequest(format!("Event {} is missing a payload", value.id)))??
            )?
        )
    }
}

impl TryFrom<StockState> for Event {
    type Error = actix_web::Error;

    fn try_from(value: StockState) -> Result<Self, Self::Error> {
        let mut event: Event = EventBuilder::default()
            .event_type("stock-decision-state.stocks-demo")
            .extensions(hashmap! {
                "stockname".to_string() => value.name.clone()
            })
            .build()
            .map_err(actix_web::error::ErrorInternalServerError)?;

        event.write_payload(
            "application/json",
            serde_json::to_value(value)?,
        )?;

        Ok(event)
    }
}
