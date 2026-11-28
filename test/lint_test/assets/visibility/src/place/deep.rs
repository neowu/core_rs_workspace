mod one;
mod two;

use super::helper;

pub(crate) fn run() -> u32 {
    one::run() + two::run() + helper::held()
}
