use std::time::Duration;

use demo::AppConfig;
use framework::load_config;
use framework::log;
use framework::log::ConsoleAppender;
use framework::system::System;
use framework_db::Database;
use framework_db::DbConfig;
use framework_db::database;

#[tokio::main]
pub async fn main() {
    let config: AppConfig = load_config!("assets/conf.json");
    let mut system = System::init(env!("CARGO_BIN_NAME"));
    system.start_action_logger(ConsoleAppender);

    let _result = log::action("migration", None, async {
        let db = Database::new(DbConfig {
            uri: config.db_url,
            user: config.db_user,
            password: config.db_password.into(),
            client: env!("CARGO_BIN_NAME"),
        })?;

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
    let _result = system.shutdown(Duration::from_secs(15)).await;
}
