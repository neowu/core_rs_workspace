use demo::AppConfig;
use framework::appender::ConsoleAppender;
use framework::load_config;
use framework::log;
use framework::system::DefaultEnv;
use framework::system::System;
use framework_db::Database;
use framework_db::DbConfig;
use framework_db::database;

#[tokio::main]
pub async fn main() {
    let config: AppConfig = load_config!("assets/conf.json");
    let system = System::init(env!("CARGO_BIN_NAME"), DefaultEnv).await.start_logger(ConsoleAppender);

    let db = Database::new(DbConfig {
        uri: config.db_url,
        user: config.db_user,
        password: config.db_password.into(),
        client: env!("CARGO_BIN_NAME"),
    });

    let _result = log::action("migration", None, async move {
        database::execute(&db, "DROP TABLE IF EXISTS public.user", &[]).await?;

        database::execute(
            &db,
            "CREATE TABLE public.user (
            id              UUID                        NOT NULL,
            name            VARCHAR(100)                NOT NULL,
            rating          INTEGER,
            tags            JSONB,
            created_date    TIMESTAMP(6) WITH TIME ZONE NOT NULL,
            PRIMARY KEY (id));",
            &[],
        )
        .await?;

        Ok(())
    })
    .await;

    system.wait().await;
    system.shutdown_logger().await;
}
