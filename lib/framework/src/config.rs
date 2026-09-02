use std::env;
use std::env::current_exe;
use std::fmt;
use std::fmt::Debug;
use std::fmt::Display;
use std::fmt::Formatter;
use std::fs::read_to_string;
use std::ops::Deref;
use std::path::PathBuf;

use serde::Deserialize;
use serde::de;
use serde::de::DeserializeOwned;

use crate::console;

/// Loads a JSON config into a deserializable type, panicking on any error.
///
/// Intended to be called once at startup. The config content comes from the
/// first available source:
///
/// 1. the environment variable named by `env`, when set and not blank, which
///    is how deployments without a writable asset dir (e.g. Cloud Run worker)
///    supply their config;
/// 2. `path` resolved next to the current exe;
/// 3. `path` resolved against `CARGO_MANIFEST_DIR`, in debug builds only.
///
/// In debug builds `.env` is loaded first, so that `env:` references in the
/// config resolve against those variables, and so that the config env var
/// itself can be set locally to exercise the deployed path; in release builds
/// that step is a no-op.
///
/// Because this only runs at startup, every failure is fatal and surfaces as a
/// panic rather than a `Result`.
///
/// ```ignore
/// let config: AppConfig = load_config!("assets/conf.json");
/// let config: AppConfig = load_config!("assets/conf.json", env = "CONFIG");
/// ```
#[macro_export]
macro_rules! load_config {
    ($path:expr) => {{ $crate::config::__load_config(None, $path, env!("CARGO_MANIFEST_DIR")) }};
    ($path:expr, env = $env:expr) => {{ $crate::config::__load_config(Some($env), $path, env!("CARGO_MANIFEST_DIR")) }};
}

#[doc(hidden)]
pub fn __load_config<T>(env_name: Option<&str>, path: &str, manifest_dir: &str) -> T
where
    T: DeserializeOwned,
{
    #[cfg(debug_assertions)]
    load_dev_env(manifest_dir);

    let json = if let Some(json) = env_name.and_then(load_from_env) {
        json
    } else {
        let config_path = resolve_config_path(path, manifest_dir);
        read_to_string(&config_path)
            .unwrap_or_else(|err| panic!("failed to read config, path={}, err={err}", config_path.display()))
    };

    console!("config:\n{json}");
    serde_json::from_str(&json).unwrap_or_else(|err| panic!("failed to parse config, err={err}"))
}

fn load_from_env(env_name: &str) -> Option<String> {
    let json = env::var(env_name).ok().filter(|json| !json.trim().is_empty())?;
    console!("load config from env, env={env_name}");
    Some(json)
}

#[cfg_attr(not(debug_assertions), allow(unused_variables))] // manifest_dir is only used with debug_assertions
fn resolve_config_path(path: &str, manifest_dir: &str) -> PathBuf {
    let exe_path = current_exe().expect("cannot get current exe path");
    let config_path = exe_path.with_file_name(path);
    if config_path.exists() {
        console!("load config from exe path, path={}", config_path.display());
        return config_path;
    }

    #[cfg(debug_assertions)]
    {
        let dev_config_path = PathBuf::from(manifest_dir).join(path);
        if dev_config_path.exists() {
            console!("load config from source code folder, path={}", dev_config_path.display());
            return dev_config_path;
        }
    }

    panic!("config not found, path={}, exe={}", config_path.display(), exe_path.display());
}

#[cfg(debug_assertions)]
fn load_dev_env(manifest_dir: &str) {
    let path = PathBuf::from(manifest_dir).join(".env");
    if !path.exists() {
        return;
    }

    let content = read_to_string(&path)
        .unwrap_or_else(|err| panic!("failed to read env file, path={}, err={err}", path.display()));

    console!("load dev env vars, path={}", path.display());
    for line in content.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let Some((key, value)) = line.split_once('=') else {
            panic!("invalid env line, path={}, line={line}", path.display());
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
    use std::fs::create_dir_all;
    use std::fs::remove_dir_all;
    use std::fs::write;
    use std::path::Path;

    use super::*;

    #[derive(Deserialize)]
    struct TestConfig {
        name: String,
    }

    #[derive(Deserialize)]
    struct TestSecretConfig {
        token: EnvString,
    }

    // each test owns its own dir, so tests stay independent while running in parallel
    fn test_dir(name: &str) -> PathBuf {
        let dir = env::temp_dir().join(format!("framework_config_test_{name}"));
        if dir.exists() {
            remove_dir_all(&dir).unwrap();
        }
        create_dir_all(dir.join("assets")).unwrap();
        dir
    }

    fn write_config(dir: &Path, json: &str) -> String {
        write(dir.join("assets/conf.json"), json).unwrap();
        "assets/conf.json".to_owned()
    }

    #[test]
    fn load_config_from_source_folder() {
        let dir = test_dir("source_folder");
        let path = write_config(&dir, r#"{"name":"from file"}"#);

        let config: TestConfig = __load_config(None, &path, dir.to_str().unwrap());

        assert_eq!(config.name, "from file");
        remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn load_config_from_env() {
        let dir = test_dir("env");
        let path = write_config(&dir, r#"{"name":"from file"}"#);
        unsafe { env::set_var("CONFIG_TEST_JSON", r#"{"name":"from env"}"#) }

        let config: TestConfig = __load_config(Some("CONFIG_TEST_JSON"), &path, dir.to_str().unwrap());

        assert_eq!(config.name, "from env"); // env wins over the file on disk
        unsafe { env::remove_var("CONFIG_TEST_JSON") }
        remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn load_config_with_blank_env() {
        let dir = test_dir("blank_env");
        let path = write_config(&dir, r#"{"name":"from file"}"#);
        unsafe { env::set_var("CONFIG_TEST_JSON_BLANK", "  ") }

        let config: TestConfig = __load_config(Some("CONFIG_TEST_JSON_BLANK"), &path, dir.to_str().unwrap());

        assert_eq!(config.name, "from file"); // a blank env var is treated as not set
        unsafe { env::remove_var("CONFIG_TEST_JSON_BLANK") }
        remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn load_config_with_unset_env() {
        let dir = test_dir("unset_env");
        let path = write_config(&dir, r#"{"name":"from file"}"#);
        unsafe { env::remove_var("CONFIG_TEST_JSON_UNSET") }

        let config: TestConfig = __load_config(Some("CONFIG_TEST_JSON_UNSET"), &path, dir.to_str().unwrap());

        assert_eq!(config.name, "from file");
        remove_dir_all(&dir).unwrap();
    }

    #[test]
    #[should_panic(expected = "config not found")]
    fn load_config_with_missing_file() {
        let dir = test_dir("missing_file");
        let _: TestConfig = __load_config(None, "assets/conf.json", dir.to_str().unwrap());
    }

    // dev env must be loaded before the env var is read, so the deployed path can be exercised locally
    #[cfg(debug_assertions)]
    #[test]
    fn load_config_from_env_resolves_dev_env() {
        let dir = test_dir("dev_env");
        let path = write_config(&dir, r#"{"token":"missing"}"#);
        write(dir.join(".env"), "CONFIG_TEST_DEV_TOKEN=dev token\n# comment\n").unwrap();
        unsafe { env::set_var("CONFIG_TEST_DEV_JSON", r#"{"token":"env:CONFIG_TEST_DEV_TOKEN"}"#) }

        let config: TestSecretConfig = __load_config(Some("CONFIG_TEST_DEV_JSON"), &path, dir.to_str().unwrap());

        assert_eq!(config.token.0, "dev token");
        unsafe { env::remove_var("CONFIG_TEST_DEV_JSON") }
        unsafe { env::remove_var("CONFIG_TEST_DEV_TOKEN") }
        remove_dir_all(&dir).unwrap();
    }

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
