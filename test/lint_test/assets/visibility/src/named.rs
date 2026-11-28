// writes items around names given by argument, so they start here and are named below
macro_rules! constants {
    ($($vis:vis const $name:ident = $value:expr;)*) => {
        $($vis const $name: u32 = $value;)*
    };
}

// lint: expect(unused_pub), set by other workspaces
pub const MARKED: u32 = 0;

constants! {
    // used by the app; the marker above is not its own
    pub const FIRST = 1;
}
