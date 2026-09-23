use std::collections::HashMap;

use framework_clickhouse::types::Decimal64;
use serde::Serialize;
use serde::Serializer;
use serde::ser::SerializeMap as _;

pub(crate) mod action_log_handler;
pub(crate) mod event_handler;
pub(crate) mod stat_handler;

// a clickhouse Map column is not nullable, so a map java core-ng left out serializes as empty
pub(crate) struct OptionMap<'a, V>(pub(crate) Option<&'a HashMap<String, V>>);

impl<V: Serialize> Serialize for OptionMap<'_, V> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut map = serializer.serialize_map(Some(self.0.map_or(0, HashMap::len)))?;
        for (key, value) in self.0.into_iter().flatten() {
            map.serialize_entry(key, value)?;
        }
        map.end()
    }
}

// java core-ng sends stats as f64 and the column is Decimal64(3), so each value is converted as it
// is written. `elapsed` is a field of its own on the message and the tables carry it as a stat, so
// it leads the map here; a message that also carries an "elapsed" stat leaves both entries, and
// clickhouse resolves the lookup to this one.
pub(crate) struct Stats<'a> {
    pub(crate) elapsed: Option<i64>,
    pub(crate) stats: Option<&'a HashMap<String, f64>>,
}

impl Serialize for Stats<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let entries = usize::from(self.elapsed.is_some()) + self.stats.map_or(0, HashMap::len);
        let mut map = serializer.serialize_map(Some(entries))?;
        if let Some(elapsed) = self.elapsed {
            map.serialize_entry("elapsed", &Decimal64::<3>::from(elapsed as f64))?;
        }
        for (key, value) in self.stats.into_iter().flatten() {
            map.serialize_entry(key, &Decimal64::<3>::from(*value))?;
        }
        map.end()
    }
}
