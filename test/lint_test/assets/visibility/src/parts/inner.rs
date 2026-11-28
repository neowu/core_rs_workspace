pub enum Kind {
    Used,
    // never named
    Unused,
    // lint: expect(unused_pub), set by other workspaces
    Kept,
    // lint: expect(unused_pub), but the app names it
    Named,
}

// the app reads the first field; only `new` names the second
pub struct Pair(pub u32, pub u32);

impl Pair {
    pub fn new(first: u32, second: u32) -> Self {
        Self(first, second)
    }
}

pub trait Source {
    fn read(&self) -> u32;

    // implemented by the app, called by no one
    fn skip(&self);
}

// used only by the sibling `b`, through the re-export
pub struct Local;
