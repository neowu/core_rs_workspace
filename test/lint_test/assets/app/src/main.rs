use visibility::parts::Kind;
use visibility::parts::Pair;
use visibility::parts::Source;
use visibility::state::Counter;

struct Zero;

impl Source for Zero {
    fn read(&self) -> u32 {
        0
    }

    fn skip(&self) {}
}

fn main() {
    visibility::entry();
    visibility::a::five();
    visibility::a::eight();
    visibility::a::nine();

    let kind = if Zero.read() == 0 { Kind::Used } else { Kind::Named };
    let _used = matches!(kind, Kind::Used);
    let _first = Pair::new(1, 2).0;

    let mut counter = Counter::new();
    visibility::state::reset(&mut counter);
    rename(&mut counter);
    add(&mut counter);
    append(&mut counter);
    let mut options = visibility::state::options();
    options.retries += 1;
    options.delay += 1;
    let _read = counter.count + counter.total + options.retries + options.delay;
}

// one change is a setter's worth
fn rename(counter: &mut Counter) {
    counter.name = String::new();
}

// two fields
fn add(counter: &mut Counter) {
    counter.count += 1;
    counter.total += 1;
}

// one field, twice
fn append(counter: &mut Counter) {
    counter.name.push('a');
    counter.name.push('b');
}
