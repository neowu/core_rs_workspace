#[tokio::main]
pub async fn main() {
    println!("{}", serde_html_form::to_string(()).unwrap());
}
