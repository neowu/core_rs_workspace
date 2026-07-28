use framework::exception::Exception;
use framework_db::Database;
use framework_db::DbConfig;

pub fn client() -> Result<Database, Exception> {
    let config = DbConfig {
        uri: "postgres://dev.internal:5432/postgres".to_owned(),
        user: "postgres".to_owned(),
        password: "postgres".to_owned(),
        client: env!("CARGO_PKG_NAME"),
    };
    Database::new(config)
}
