#[tokio::main]
pub async fn main() {
    let mut vec = vec![1];
    vec.push(2);
    println!("{:?}", vec.pop());

    println!("shutdown");
}
