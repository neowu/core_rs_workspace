use std::env;
use std::env::current_exe;
use std::fmt;
use std::fmt::Debug;
use std::fmt::Display;
use std::fmt::Formatter;
use std::fs::read_to_string;
use std::ops::Deref;
use std::path::Path;
use std::path::PathBuf;

use serde::Deserialize;
use serde::de;
use serde::de::DeserializeOwned;

use crate::console;

/// Loads a JSON config file into a deserializable type, panicking on any error.
///
/// Intended to be called once at startup. In debug builds it first loads
/// `.env` (see [`load_env!`]) so that `env:` references in the config resolve
/// against those variables; in release builds the `.env` step is a no-op. The
/// config path is resolved with [`asset_path!`], which also panics if the file
/// is not found.
///
/// Because this only runs at startup, every failure is fatal and surfaces as a
/// panic rather than a `Result`.
#[macro_export]
macro_rules! load_config {
    ($path:expr) => {{ $crate::config::__load_config($path, env!("CARGO_MANIFEST_DIR")) }};
}

#[doc(hidden)]
#[cfg_attr(not(debug_assertions), allow(unused_variables))]
pub fn __load_config<T>(path: &'static str, manifest_dir: &'static str) -> T
where
    T: DeserializeOwned,
{
    let exe_path = current_exe().expect("cannot get current exe path");
    let config_path = exe_path.with_file_name(path);
    if config_path.exists() {
        console!("load config from exe path, path={}", config_path.display());
        return parse_config(&config_path);
    }

    #[cfg(debug_assertions)]
    {
        use std::path::PathBuf;

        let dev_config_path = PathBuf::from(manifest_dir).join(path);
        if dev_config_path.exists() {
            load_dev_env(manifest_dir);
            console!("load config from source code folder, path={}", dev_config_path.display());
            return parse_config(&dev_config_path);
        }
    }

    panic!("config not found, path={}, exe={}", config_path.display(), exe_path.display());
}

fn parse_config<T>(path: &Path) -> T
where
    T: DeserializeOwned,
{
    let json =
        read_to_string(path).unwrap_or_else(|err| panic!("failed to read file, path={}, err={err}", path.display()));
    serde_json::from_str(&json).unwrap_or_else(|err| panic!("failed to deserialize, json={json}, err={err}"))
}

#[cfg(debug_assertions)]
fn load_dev_env(manifest_dir: &str) {
    let path = PathBuf::from(manifest_dir).join(".env");
    if !path.exists() {
        return;
    }

    let content = read_to_string(&path)
        .unwrap_or_else(|err| panic!("failed to read env file, file={}, err={}", path.display(), err));

    console!("load env vars, file={}", path.display());
    for line in content.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let Some((key, value)) = line.split_once('=') else {
            panic!("invalid env line, file={}, line={line}", path.display());
        };
        unsafe {
            env::set_var(key.trim(), value.trim());
        }
    }
}

/// A string configuration loaded inline or from an environment variable.
///
/// The raw JSON value is always a string: if it starts with `env:`, the suffix
/// names an environment variable read at resolution time; otherwise the
/// string itself is the literal.
///
/// # JSON forms
///
/// ```json
/// { "token": "abc123" }
/// { "token": "env:API_TOKEN" }
/// ```
pub struct EnvString(String);

impl<'de> Deserialize<'de> for EnvString {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let raw = String::deserialize(deserializer)?;
        let resolved = if let Some(key) = raw.strip_prefix("env:") {
            env::var(key).map_err(|err| de::Error::custom(format!("failed to load from env, env={key}, err={err}")))?
        } else {
            raw
        };
        Ok(EnvString(resolved))
    }
}

impl Display for EnvString {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl Debug for EnvString {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        Display::fmt(&self, f)
    }
}

impl Deref for EnvString {
    type Target = String;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl From<EnvString> for String {
    fn from(env: EnvString) -> Self {
        env.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn env_string_with_literal_value() {
        let string: EnvString = serde_json::from_str(r#""value""#).unwrap();
        assert_eq!(string.0, "value");
    }

    #[test]
    fn env_string_with_env_var() {
        unsafe { env::set_var("CONFIG_TEST_SECRET", "secret") }
        let secret: EnvString = serde_json::from_str(r#""env:CONFIG_TEST_SECRET""#).unwrap();
        assert_eq!(secret.0, "secret");
        unsafe { env::remove_var("CONFIG_TEST_SECRET") }
    }

    #[test]
    fn env_string_with_missing_env_var() {
        unsafe { env::remove_var("CONFIG_TEST_SECRET_MISSING") }
        let err = serde_json::from_str::<EnvString>(r#""env:CONFIG_TEST_SECRET_MISSING""#).unwrap_err();
        assert!(err.to_string().contains("failed to load from env, env=CONFIG_TEST_SECRET_MISSING"));
    }

    #[test]
    fn env_string_display_debug() {
        let string: EnvString = serde_json::from_str(r#""value""#).unwrap();
        assert_eq!(format!("{string}"), "value");
        assert_eq!(format!("{string:?}"), "value");
    }
}
