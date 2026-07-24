use framework::exception::Exception;
use framework::log;
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

// log::init panics if called twice, but every test in a binary shares the process
static LOG_INIT: std::sync::Once = std::sync::Once::new();

pub async fn run_test(name: &'static str, task: impl Future<Output = Result<(), Exception>>) {
    LOG_INIT.call_once(|| log::init("console", env!("CARGO_PKG_NAME")));
    log::action(name, None, task).await.unwrap();
}
