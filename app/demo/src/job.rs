use axum::Router;
use axum::debug_handler;
use axum::extract::State;
use axum::http::StatusCode;
use axum::routing::post;
use chrono::Utc;
use framework::exception::Exception;
use framework::schedule::JobContext;
use framework::web::error::HttpResult;

use crate::AppState;

pub fn routes() -> Router<&'static AppState> {
    Router::new().route("/job/demo_job", post(run_demo_job))
}

#[debug_handler]
async fn run_demo_job(State(state): State<&'static AppState>) -> HttpResult<StatusCode> {
    demo_job(
        state,
        JobContext {
            name: "demo_job",
            scheduled_time: Utc::now(),
        },
    )
    .await?;
    Ok(StatusCode::ACCEPTED)
}

pub async fn demo_job(_state: &AppState, context: JobContext) -> Result<(), Exception> {
    println!("run demo job, scheduled_time={}", context.scheduled_time);
    Ok(())
}
