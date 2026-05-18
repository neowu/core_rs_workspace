use std::str::FromStr;

use chrono::DateTime;
use chrono::Utc;
use demo::AppConfig;
use framework::asset_path;
use framework::exception::Exception;
use framework::json;
use framework::load_env;
use framework::log;
use framework::log::appender::ConsoleAppender;
use framework_db::DbConfig;
use framework_db::Field;
use framework_db::database;
use framework_db::repository;
use framework_macro::Entity;
use tracing::debug;
use tracing::warn;
use uuid::Uuid;

#[allow(unused)]
#[derive(Entity, Debug)]
#[table(name = "user")]
pub(crate) struct User {
    #[primary_key]
    #[column(name = "id")]
    id: i32,
    #[column(name = "name")]
    name: String,
    #[column(name = "col1")]
    col1: Option<String>,
}

#[allow(unused)]
#[derive(Entity, Debug)]
#[table(name = "user_profile")]
pub(crate) struct UserProfile {
    #[primary_key]
    #[column(name = "id1")]
    id1: String,
    #[primary_key]
    #[column(name = "id2")]
    id2: Uuid,
    #[column(name = "name")]
    name: String,
    #[column(name = "rating")]
    rating: Option<i32>,
}

#[allow(unused)]
#[derive(Entity, Debug)]
#[table(name = "orders")]
pub(crate) struct Order {
    #[primary_key(auto_increment)]
    #[column(name = "id")]
    id: Option<i64>,
    #[column(name = "date")]
    date: Option<DateTime<Utc>>,
}

#[tokio::main]
pub async fn main() -> Result<(), Exception> {
    log::init_with_action(ConsoleAppender);
    load_env!(".env")?;

    let config: AppConfig = json::load_file(&asset_path!("assets/conf.json")?)?;

    let db = framework_db::Database::new(DbConfig {
        uri: config.db_url,
        user: config.db_user,
        password: config.db_password.into(),
        client: env!("CARGO_BIN_NAME"),
    })?;

    log::start_action("test-db", None, async {
        warn!("trigger");

        // let profile = UserProfile { id1: "id1".to_string(), id2: Uuid::now_v7(), name: "neo".to_string() };
        // db::repository::insert(&db, &profile).await?;
        let profile = repository::get::<UserProfile>(
            &db,
            &("id1".to_string(), Uuid::from_str("019dd6c2-3fe8-7501-a1a0-e69dc7c60346")?),
        )
        .await?;
        debug!("{profile:?}");

        let profiles = repository::select_all::<UserProfile>(
            &db,
            vec![
                UserProfile::FIELDS.name.is_in(vec![&"neo".to_owned(), &"neo2".to_owned()]),
                UserProfile::FIELDS.name.eq(&"neo".to_owned()),
                UserProfile::FIELDS.name.not_null(),
            ],
        )
        .await?;
        debug!("{profiles:?}");

        repository::update(
            &db,
            &(profiles[0].id1.clone(), profiles[0].id2),
            vec![UserProfile::FIELDS.rating.update(&Some(5))],
        )
        .await?;

        repository::update_all(
            &db,
            vec![UserProfile::FIELDS.rating.update(&Some(3))],
            vec![UserProfile::FIELDS.name.eq(&"neo".to_owned())],
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
