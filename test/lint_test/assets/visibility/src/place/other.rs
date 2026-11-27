use super::helper;

pub(crate) fn run() -> u32 {
    helper::shared() + helper::excused()
}
