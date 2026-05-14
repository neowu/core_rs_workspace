use std::env;
use std::fmt::Display;
use std::str::FromStr;

use serde::Deserialize;

use crate::exception::Exception;

#[derive(Debug, Deserialize)]
#[serde(untagged)]
pub enum ConfigValue<T> {
    Literal(T),
    FromEnv { env: String },
}

impl<T: Clone + FromStr> ConfigValue<T>
where
    T::Err: Display,
{
    pub fn value(self) -> Result<T, Exception> {
        match self {
            ConfigValue::Literal(value) => Ok(value),
            ConfigValue::FromEnv { env } => match env::var(&env) {
                Ok(value) => {
                    value.parse().map_err(|err| exception!(message = format!("failed to parse, env={env}, err={err}")))
                }
                Err(err) => Err(exception!(message = format!("failed to load from env, env={env}"), source = err)),
            },
        }
    }
}
