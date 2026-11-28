mod deep;
mod helper;
mod other;
mod table;
mod user;

pub(crate) fn run() -> u32 {
    user::run() + other::run() + deep::run()
}
