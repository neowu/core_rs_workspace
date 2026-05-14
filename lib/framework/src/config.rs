use std::env;
use std::fmt::Display;
use std::str::FromStr;

use serde::Deserialize;

use crate::exception::Exception;

/// A configuration field that can be set inline or sourced from an environment variable.
///
/// Used to load service configuration at startup from JSON, while allowing
/// any field to be overridden by an environment variable lookup. The `FromEnv`
/// form is detected by the presence of an `env` key in the JSON object.
///
/// # JSON forms
///
/// ```json
/// { "port": 8080 }
/// { "port": { "env": "PORT" } }
/// ```
///
/// # Example
///
/// ```ignore
/// #[derive(Deserialize)]
/// struct Config {
///     port: ConfigValue<u16>,
/// }
///
/// let config: Config = serde_json::from_str(json)?;
/// let port: u16 = config.port.value()?;
/// ```
#[derive(Debug, Deserialize)]
#[serde(untagged)]
pub enum ConfigValue<T> {
    /// The value is provided directly in the configuration.
    Literal(T),
    /// The value must be resolved from the named environment variable at load time.
    FromEnv { env: String },
}

impl<T: Clone + FromStr> ConfigValue<T>
where
    T::Err: Display,
{
    /// Resolve the configured value.
    ///
    /// For `Literal`, returns the inline value as-is. For `FromEnv`, reads the
    /// named environment variable and parses it via [`FromStr`].
    ///
    /// # Errors
    ///
    /// Returns an [`Exception`] if the environment variable is unset, or if
    /// its value cannot be parsed into `T`.
    pub fn value(self) -> Result<T, Exception> {
        match self {
            ConfigValue::Literal(value) => Ok(value),
            ConfigValue::FromEnv { env } => match env::var(&env) {
                Ok(value) => {
                    value.parse().map_err(|err| exception!(format!("failed to parse, env={env}, err={err}")))
                }
                Err(err) => Err(exception!(format!("failed to load from env, env={env}"), source = err)),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn literal_value() {
        let config: ConfigValue<u16> = serde_json::from_str("8080").unwrap();
        assert_eq!(config.value().unwrap(), 8080);
    }

    #[test]
    fn literal_string_value() {
        let config: ConfigValue<String> = serde_json::from_str(r#""hello""#).unwrap();
        assert_eq!(config.value().unwrap(), "hello");
    }

    #[test]
    fn from_env_value() {
        // SAFETY: unique env var name avoids races with other tests
        unsafe { env::set_var("CONFIG_TEST_PORT", "9090") }
        let config: ConfigValue<u16> = serde_json::from_str(r#"{"env":"CONFIG_TEST_PORT"}"#).unwrap();
        assert_eq!(config.value().unwrap(), 9090);
        unsafe { env::remove_var("CONFIG_TEST_PORT") }
    }

    #[test]
    fn from_env_missing() {
        unsafe { env::remove_var("CONFIG_TEST_MISSING") }
        let config: ConfigValue<u16> = serde_json::from_str(r#"{"env":"CONFIG_TEST_MISSING"}"#).unwrap();
        let err = config.value().unwrap_err();
        assert!(err.message.contains("failed to load from env, env=CONFIG_TEST_MISSING"));
    }

    #[test]
    fn from_env_parse_failure() {
        unsafe { env::set_var("CONFIG_TEST_BAD", "not_a_number") }
        let config: ConfigValue<u16> = serde_json::from_str(r#"{"env":"CONFIG_TEST_BAD"}"#).unwrap();
        let err = config.value().unwrap_err();
        assert!(err.message.contains("failed to parse, env=CONFIG_TEST_BAD"));
        unsafe { env::remove_var("CONFIG_TEST_BAD") }
    }

    #[test]
    fn nested_in_struct() {
        #[derive(Deserialize)]
        struct Config {
            name: ConfigValue<String>,
            port: ConfigValue<u16>,
        }
        unsafe { env::set_var("CONFIG_TEST_NAME", "service-a") }
        let json = r#"{"port": 8080, "name": {"env": "CONFIG_TEST_NAME"}}"#;
        let config: Config = serde_json::from_str(json).unwrap();
        assert_eq!(config.port.value().unwrap(), 8080);
        assert_eq!(config.name.value().unwrap(), "service-a");
        unsafe { env::remove_var("CONFIG_TEST_NAME") }
    }
}
