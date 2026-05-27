use std::collections::HashMap;
use std::sync::Arc;

use axum::Router;
use axum::extract::Path;
use axum::extract::State;
use axum::routing::put;
use chrono::Utc;
use http::StatusCode;

use crate::exception::Severity;
use crate::exception::error_code;
use crate::schedule::JobContext;
use crate::schedule::Schedule;
use crate::schedule::Scheduler;
use crate::task;
use crate::web::error::HttpResult;

#[derive(Clone)]
struct JobState<S> {
    state: S,
    schedules: Arc<HashMap<&'static str, Arc<Schedule<S>>>>,
}

async fn run_job<S>(State(state): State<JobState<S>>, Path(job): Path<String>) -> HttpResult<StatusCode>
where
    S: Clone,
{
    let schedule = state.schedules.get(job.as_str()).ok_or_else(|| {
        exception!(format!("job not found, name={job}"), severity = Severity::Warn, code = error_code::NOT_FOUND)
    })?;
    let context = JobContext { name: schedule.name, scheduled_time: Utc::now() };
    task::spawn((schedule.job)(state.state.clone(), context));
    Ok(StatusCode::ACCEPTED)
}

pub trait SystemRoute<S> {
    fn routes(&self, state: S) -> Router;
}

impl<S> SystemRoute<S> for Scheduler<S>
where
    S: Clone + Send + Sync + 'static,
{
    fn routes(&self, state: S) -> Router {
        let jobs: HashMap<&'static str, Arc<Schedule<S>>> =
            self.schedules.iter().map(|schedule| (schedule.name, Arc::clone(schedule))).collect();
        Router::new().route("/_sys/job/{job}", put(run_job)).with_state(JobState { state, schedules: Arc::new(jobs) })
    }
}
