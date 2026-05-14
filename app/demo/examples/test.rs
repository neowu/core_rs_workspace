use std::pin::Pin;

type Handle = Box<dyn Fn() -> Pin<Box<dyn Future<Output = ()> + Send>>>;

struct Entity {
    handle: Handle,
}

#[tokio::main]
pub async fn main() {
    tokio::spawn(async move {
        let fut = {
            let entity = Entity { handle: Box::new(test) };
            (entity.handle)()
        };
        fut.await
    })
    .await
    .unwrap();
}

fn test() -> Pin<Box<dyn Future<Output = ()> + Send>> {
    Box::pin(async move { println!("test") })
}
