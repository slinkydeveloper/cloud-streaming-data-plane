use cloudevent::Event;
use faas_rust_macro::faas_function;

#[faas_function]
pub async fn log(input: Event) -> Result<Option<Event>, actix_web::Error> {
    println!("{:#?}", input);
    Ok(None)
}
