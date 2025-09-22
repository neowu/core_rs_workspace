use std::fmt::Debug;
use std::fs::read_to_string;
use std::path::Path;

use serde::Serialize;
use serde::de::Deserialize;
use serde::de::DeserializeOwned;

use crate::exception::CoreRsResult;

pub fn load_file<T>(path: &Path) -> CoreRsResult<T>
where
    T: DeserializeOwned,
{
    let json = read_to_string(path).map_err(|err| {
        exception!(
            message = format!("failed to read file, path={}", path.to_string_lossy()),
            source = err
        )
    })?;
    serde_json::from_str(&json)
        .map_err(|err| exception!(message = format!("failed to deserialize, json={json}"), source = err))
}

pub fn from_json<'a, T>(json: &'a str) -> CoreRsResult<T>
where
    T: Deserialize<'a>,
{
    serde_json::from_str(json)
        .map_err(|err| exception!(message = format!("failed to deserialize, json={json}"), source = err))
}

pub fn to_json<T>(object: &T) -> CoreRsResult<String>
where
    T: Serialize + Debug,
{
    serde_json::to_string(object).map_err(|err| {
        exception!(
            message = format!("failed to serialize, object={object:?}"),
            source = err
        )
    })
}

pub fn to_json_value<T>(enum_value: &T) -> String
where
    T: Serialize + Debug,
{
    if let Ok(value) = serde_json::to_string(enum_value) {
        value[1..value.len() - 1].to_string()
    } else {
        Default::default()
    }
}
