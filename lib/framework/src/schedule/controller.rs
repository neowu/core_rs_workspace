use std::collections::HashMap;
use std::sync::Arc;

use http::StatusCode;
use serde::Deserialize;

use crate::exception::Exception;
use crate::exception::error_code;
use crate::log::Severity;
use crate::schedule::JobContext;
use crate::schedule::Schedule;
use crate::schedule::Scheduler;
use crate::task::TaskExecutor;
use crate::time::DateTime;
use crate::time::Offset;
use crate::web::request::Request;
use crate::web::response::Response;
use crate::web::router::Router;

pub struct JobState<S> {
    state: S,
    timezone: Offset,
    schedules: HashMap<&'static str, Arc<Schedule<S>>>,
    executor: Arc<TaskExecutor>,
}

#[derive(Deserialize)]
struct TriggerJobRequest {
    job: String,
}

async fn trigger_job<S>(state: Arc<JobState<S>>, mut request: Request) -> Result<Response, Exception>
where
    S: Clone,
{
    let TriggerJobRequest { job } = request.json().await?;
    let schedule = state.schedules.get(job.as_str()).ok_or_else(|| {
        exception!(format!("job not found, name={job}"), severity = Severity::Warn, code = error_code::NOT_FOUND)
    })?;
    let context = JobContext { name: schedule.name, scheduled_time: DateTime::now().with_timezone(state.timezone) };
    state.executor.spawn(schedule.name, (schedule.job)(state.state.clone(), context));
    Ok(Response::empty().status(StatusCode::ACCEPTED))
}

impl<S> Scheduler<S>
where
    S: Clone + Send + Sync + 'static,
{
    /// `PUT /_sys/job/trigger` with `{"job": "name"}` triggers a job, merge into the http server router.
    pub fn routes(&self, state: S) -> Router {
        let schedules = self.schedules.iter().map(|schedule| (schedule.name, Arc::clone(schedule))).collect();
        let state = JobState { state, timezone: self.timezone, schedules, executor: Arc::clone(&self.executor) };
        Router::new().state(Arc::new(state), |r| r.put("/_sys/job/trigger", trigger_job))
    }
}
