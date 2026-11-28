use super::helper;
use super::table;

pub(crate) fn run() -> u32 {
    helper::shared() + helper::excused() + table::FIRST
}
