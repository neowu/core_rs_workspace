use std::any::type_name;
use std::fmt::Display;

struct Container<T> {
    value: T,
}

fn test<T>(c: Container<T>)
where
    T: Display,
{
    println!("{}{}", type_name::<T>(), c.value);
}

#[tokio::main]
pub async fn main() {
    test(Container { value: 43 });
}
