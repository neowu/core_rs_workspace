use framework::exception::Exception;
use framework::log;

#[tokio::main]
pub async fn main() -> Result<(), Exception> {
    log::init();

    let x = tracing::Level::ERROR.le(&tracing::Level::WARN);
    dbg!(x);

    Ok(())
}
