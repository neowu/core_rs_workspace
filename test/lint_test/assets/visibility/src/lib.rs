pub mod a;
mod b;
pub mod parts;
pub mod state;

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
