use std::env;
use std::fmt;
use std::fmt::Debug;
use std::fmt::Display;
use std::fmt::Formatter;
use std::ops::Deref;

use serde::Deserialize;
use serde::de;

use crate::exception::Exception;

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
///
/// # Example
///
/// ```ignore
/// #[derive(Deserialize)]
/// struct Config {
///     token: EnvString,
/// }
///
/// let config: Config = serde_json::from_str(json)?;
/// let token: String = config.token.value();
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

/// Loads environment variables from a file in debug builds; no-op in release.
///
/// The file is parsed line-by-line as `KEY=VALUE`. Blank lines and lines
/// starting with `#` are ignored. The first `=` is the separator; later `=`
/// characters stay in the value. Whitespace around the key and value is
/// trimmed. Existing env vars are always overwritten.
///
/// The path is resolved relative to the caller crate's `CARGO_MANIFEST_DIR`.
///
/// # Example
///
/// ```ignore
/// #[tokio::main]
/// async fn main() -> Result<(), Exception> {
///     framework::load_env!(".env")?;
///     // ...
/// }
/// ```
#[macro_export]
macro_rules! load_env {
    ($path:expr) => {
        $crate::config::__load_env(env!("CARGO_MANIFEST_DIR"), $path)
    };
}

#[doc(hidden)]
pub fn __load_env(manifest_dir: &str, path: &str) -> Result<(), Exception> {
    #[cfg(debug_assertions)]
    {
        use std::fs::read_to_string;
        use std::path::PathBuf;

        let file_path = PathBuf::from(manifest_dir).join(path);
        let contents = read_to_string(&file_path).map_err(|err| {
            exception!(format!("failed to read env file, file={}", file_path.to_string_lossy()), source = err)
        })?;
        tracing::info!("load env vars, file={}", file_path.to_string_lossy());
        for line in contents.lines() {
            let line = line.trim();
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            let Some((key, value)) = line.split_once('=') else {
                return Err(exception!(format!("invalid env line, path={}, line={line}", file_path.to_string_lossy())));
            };
            unsafe {
                env::set_var(key.trim(), value.trim());
            }
        }
    }
    Ok(())
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
