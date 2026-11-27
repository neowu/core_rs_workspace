pub mod a;
mod b;

pub fn entry() {
    b::run();
}

#[cfg(test)]
mod tests {
    #[test]
    fn seven() {
        super::a::seven();
    }
}
