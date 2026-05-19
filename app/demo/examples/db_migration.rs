use demo::AppConfig;
use framework::asset_path;
use framework::json;
use framework::load_env;
use framework::log;
use framework::log::appender::ConsoleAppender;
use framework_db::Database;
use framework_db::DbConfig;
use framework_db::database;

#[tokio::main]
pub async fn main() {
    log::init_with_action(ConsoleAppender);

    log::start_action("migration", None, async {
        load_env!(".env")?;
        let config: AppConfig = json::load_file(&asset_path!("assets/conf.json")?)?;

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
}
