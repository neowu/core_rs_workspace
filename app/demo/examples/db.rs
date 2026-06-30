use std::str::FromStr;

use demo::AppConfig;
use demo::user::User;
use framework::exception::Exception;
use framework::load_config;
use framework::log;
use framework::warn;
use framework_db::DbConfig;
use framework_db::Field;
use framework_db::database;
use framework_db::repository;
use uuid::Uuid;

#[tokio::main]
pub async fn main() -> Result<(), Exception> {
    let config: AppConfig = load_config!("assets/conf.json");
    log::init(&config.log_appender, env!("CARGO_PKG_NAME"));

    let db = framework_db::Database::new(DbConfig {
        uri: config.db_url,
        user: config.db_user,
        password: config.db_password.into(),
        client: env!("CARGO_BIN_NAME"),
    })?;

    let _ = log::action("test-db", None, async {
        warn!(error_code = "TRIGGER", "trigger");

        // let profile = UserProfile { id1: "id1".to_string(), id2: Uuid::now_v7(), name: "neo".to_string() };
        // db::repository::insert(&db, &profile).await?;
        let user = repository::get::<User>(&db, &Uuid::from_str("019dd6c2-3fe8-7501-a1a0-e69dc7c60346")?).await?;
        log!("{user:?}");

        let users = repository::select_all::<User>(
            &db,
            vec![
                User::FIELDS.name.is_in(vec![&"neo".to_owned(), &"neo2".to_owned()]),
                User::FIELDS.name.eq(&"neo".to_owned()),
                User::FIELDS.name.not_null(),
            ],
        )
        .await?;
        log!("{users:?}");

        repository::update_all(
            &db,
            vec![User::FIELDS.rating.update(Some(3))],
            vec![User::FIELDS.name.eq(&"neo".to_owned())],
        )
        .await?;

        // let user = db::select_one::<User>(&db, r#"SELECT * from "user" where id = $1"#, &[&11]).await?;

        // debug!("{user:?}");

        // if let Some(mut user) = user {
        //     user.id += 2;
        //     // db::repository::insert_ignore(&db, &user).await?;
        //     let inserted = db::repository::upsert(&db, &user).await?;
        //     debug!("insert => {inserted}");

        //     let deleted = db::repository::delete::<User>(&db, &[&user.id]).await?;
        //     debug!("deleted => {deleted}");
        // }
        // {
        //     let order = Order { id: None, date: Some(Utc::now()) };
        //     let id = db::repository::insert_with_auto_increment_id(&db1, &order).await.unwrap();
        //     debug!("id = {id}");
        // }
        //
        let x = "hello".to_string();
        let count = database::select_one::<i64>(&db, "select count(1) from \"user\" where name = $1", &[&x]).await?;
        println!("{count:?}");

        // let orders = db::repository::select::<User>(&db, "", &[]).await?;
        // debug!("orders = {orders:?}");

        // let order = db::repository::get::<User>(&db, &11).await?;
        // debug!("user = {order:?}");

        // let order = db::repository::get::<Order>(&db, &13i64).await?;
        // debug!("user = {order:?}");

        // let user = User { id: 11, name: "name_10".to_owned(), col1: None };
        // db::repository::insert(&db, &user).await?;

        // let one: i32 = client.query_one_scalar("select 1", &[]).await?;
        // println!("{one}, closed={}, sleeping", client.is_closed());
        // tokio::time::sleep(Duration::from_secs(30)).await;
        // let one = client.query_one_scalar::<i32, _>("select 1", &[]).await;
        // println!("{:?}, closed={}, end", one.err(), client.is_closed());

        Ok(())
    })
    .await;

    Ok(())
}
