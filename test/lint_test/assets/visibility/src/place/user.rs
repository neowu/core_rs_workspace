use super::helper;

pub(crate) fn run() -> u32 {
    helper::lone() + helper::shared() + helper::rooted() + helper::KEPT + helper::excused()
}
