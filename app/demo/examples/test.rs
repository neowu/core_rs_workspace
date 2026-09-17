struct Registry<'a> {
    keys: Vec<&'a str>,
}

impl<'a> Registry<'a> {
    fn add(&mut self, key: &'a str) {
        self.keys.push(key);
    }
}

fn main() {
    let mut r = Registry { keys: Vec::new() };
    r.add("subject"); // &'static str
    r.add("client"); // &'static str
}
