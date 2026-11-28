use super::helper;
use super::table;

pub(crate) fn run() -> u32 {
    helper::lone() + helper::shared() + helper::rooted() + helper::KEPT + helper::excused() + table::FIRST + table::SECOND
}
