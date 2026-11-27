mod helper;
mod other;
mod user;

pub(crate) fn run() -> u32 {
    user::run() + other::run()
}
