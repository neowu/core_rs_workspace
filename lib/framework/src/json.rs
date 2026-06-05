use std::fmt::Debug;

use serde::Serialize;
use serde::de::Deserialize;

use crate::exception::Exception;

pub fn from_json<'a, T>(json: &'a str) -> Result<T, Exception>
where
    T: Deserialize<'a>,
{
    serde_json::from_str(json).map_err(|err| exception!(format!("failed to deserialize, json={json}"), source = err))
}

pub fn to_json<T>(object: &T) -> Result<String, Exception>
where
    T: Serialize + Debug,
{
    serde_json::to_string(object)
        .map_err(|err| exception!(format!("failed to serialize, object={object:?}"), source = err))
}

pub fn to_json_value<T>(enum_value: &T) -> String
where
    T: Serialize + Debug,
{
    if let Ok(value) = serde_json::to_string(enum_value) {
        value[1..value.len() - 1].to_string()
    } else {
        String::default()
    }
}
