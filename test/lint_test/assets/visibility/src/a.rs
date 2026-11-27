// used only inside this module
pub fn one() {}

// lint: expect(unused_pub), API for other workspaces
#[inline]
pub fn two() {}

pub(super) fn three() {}

// used by the sibling `b`, `Shown` only through its signature
pub fn four() -> Shown {
    Shown
}

pub struct Shown;

// used by the app
pub fn five() {
    one();
    three();
}

pub fn six() {}

// used only by the unit tests, which stand for no caller
pub fn seven() {}

// lint: expect(unused_pub), but the app uses it
pub fn eight() {}

// lint: expect(excess_visibility), but the app needs it
pub fn nine() {}

// lint: expect(excess_visibility), set by other workspaces
pub fn ten() {}

// used only by the tests of `e2e`, which stand for callers
pub fn eleven() {}
