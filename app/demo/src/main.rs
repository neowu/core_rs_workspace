use framework::exception::Exception;

#[tokio::main]
async fn main() -> Result<(), Exception> {
    demo::run().await
}
