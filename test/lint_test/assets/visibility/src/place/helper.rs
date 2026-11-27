// only `user` calls it
pub(crate) fn lone() -> u32 {
    1
}

// `user` and `other` call it
pub(crate) fn shared() -> u32 {
    2
}

// needs what only this module sees
pub(crate) fn rooted() -> u32 {
    hidden()
}

fn hidden() -> u32 {
    3
}

// lint: expect(misplaced_item), read with the others here
pub(crate) const KEPT: u32 = 4;

// lint: expect(misplaced_item), but two modules call it
pub(crate) fn excused() -> u32 {
    5
}
