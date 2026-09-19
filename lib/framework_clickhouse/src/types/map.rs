use serde::Serialize;
use serde::Serializer;
use serde::ser::SerializeMap as _;

// maps to a clickhouse Map(K, V) column from an ordered slice of pairs, so a row can point at the
// message's own (key, value) list instead of collecting a HashMap first: nothing is allocated and
// no key is hashed on the way in. RowBinary carries a length followed by the pairs, which is what
// serde's map protocol emits, and clickhouse stores a Map as an array of tuples, so the slice's
// order survives into the column. A repeated key is written as-is, and a lookup finds the first.
#[derive(Debug)]
pub struct Map<'a, K, V>(pub &'a [(K, V)]);

impl<K: AsRef<str>, V: Serialize> Serialize for Map<'_, K, V> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut map = serializer.serialize_map(Some(self.0.len()))?;
        for (key, value) in self.0 {
            map.serialize_entry(key.as_ref(), value)?;
        }
        map.end()
    }
}

#[cfg(test)]
mod tests {
    use framework::json;

    use super::Map;

    #[test]
    fn serialize_in_slice_order() {
        let stats = [("elapsed".to_owned(), 42_u64), ("db_count".to_owned(), 1)];
        assert_eq!(json::to_json(&Map(&stats)).unwrap(), r#"{"elapsed":42,"db_count":1}"#);
    }

    #[test]
    fn serialize_empty() {
        let stats: [(String, u64); 0] = [];
        assert_eq!(json::to_json(&Map(&stats)).unwrap(), "{}");
    }
}
