use crate::place::helper;

pub(crate) fn run() -> u32 {
    helper::spread() + helper::held()
}
