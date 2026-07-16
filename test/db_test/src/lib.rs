use framework::exception::Exception;
use framework::log;
use framework_db::Database;
use framework_db::DbConfig;

pub fn setup() -> Result<Database, Exception> {
    log::init("console", env!("CARGO_PKG_NAME"));

    let config = DbConfig {
        uri: "postgres://dev.internal:5432/test".to_owned(),
        user: "postgres".to_owned(),
        password: "postgres".to_owned(),
        client: env!("CARGO_PKG_NAME"),
    };

    Database::new(config)
}
