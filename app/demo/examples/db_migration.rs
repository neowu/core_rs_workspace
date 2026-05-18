use demo::AppConfig;
use framework::asset_path;
use framework::exception::Exception;
use framework::json;
use framework::load_env;
use framework::log;
use framework::log::appender::ConsoleAppender;
use framework_db::DbConfig;

#[tokio::main]
pub async fn main() -> Result<(), Exception> {
    log::init_with_action(ConsoleAppender);
    load_env!(".env")?;
    let config: AppConfig = json::load_file(&asset_path!("assets/conf.json")?)?;

    let _db = framework_db::Database::new(DbConfig {
        uri: config.db_url,
        user: config.db_user,
        password: config.db_password.into(),
        client: env!("CARGO_BIN_NAME"),
    })?;

    Ok(())
}
