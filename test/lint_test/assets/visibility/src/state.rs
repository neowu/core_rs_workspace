// lint: enforce(field_write), the counts are kept in step
pub struct Counter {
    pub count: u32,
    pub total: u32,
    pub name: String,
}

impl Counter {
    pub fn new() -> Self {
        Self { count: 0, total: 0, name: String::new() }
    }
}

// its own module changes it as it needs
pub fn reset(counter: &mut Counter) {
    counter.count = 0;
    counter.total = 0;
}

// plain data, not enforced
pub struct Options {
    pub retries: u32,
    pub delay: u32,
}

// lint: enforce(field_write), but a function has no fields
pub fn options() -> Options {
    Options { retries: 0, delay: 0 }
}

// lint: allow(unused_pub), a verb that does not exist
// lint: expect(unused_code), a rule that does not exist
// lint: expect(excess_visibility), but a private function is not checked
fn hidden() {}
