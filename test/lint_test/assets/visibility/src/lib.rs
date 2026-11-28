pub mod a;
mod b;
pub mod named;
pub mod parts;
mod place;
pub mod state;

pub fn entry() {
    b::run();
    place::run();
}

#[cfg(test)]
mod tests {
    #[test]
    fn seven() {
        super::a::seven();
    }
}
