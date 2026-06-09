use std::any::type_name;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use chrono::DateTime;
use chrono::FixedOffset;
use chrono::NaiveTime;
use chrono::SecondsFormat;
use chrono::Utc;
use futures::FutureExt as _;
use tokio::task::JoinSet;
use tokio::time;
use tokio_util::sync::CancellationToken;

use crate::exception::Exception;
use crate::log;
use crate::log::current_action_id;
use crate::schedule::trigger::Trigger;
use crate::task;

pub mod controller;
mod trigger;

pub struct JobContext {
    pub name: &'static str,
    pub scheduled_time: DateTime<Utc>,
}

type Job<S> = Box<dyn Fn(S, JobContext) -> Pin<Box<dyn Future<Output = ()> + Send>> + Send + Sync>;

struct Schedule<S> {
    name: &'static str,
    job: Job<S>,
    trigger: Trigger,
}

pub struct Scheduler<S> {
    timezone: FixedOffset,
    schedules: Vec<Arc<Schedule<S>>>,
}

impl<S> Scheduler<S>
where
    S: Send + Sync + 'static,
{
    pub const fn new(timezone: FixedOffset) -> Self {
        Self { timezone, schedules: Vec::new() }
    }

    pub fn schedule_fixed_rate<J, Fut>(&mut self, name: &'static str, job: J, interval: Duration)
    where
        J: Fn(S, JobContext) -> Fut + Copy + Send + Sync + 'static,
        Fut: Future<Output = Result<(), Exception>> + Send + 'static,
    {
        self.add_job(name, job, Trigger::FixedRate(interval));
    }

    pub fn schedule_daily<J, Fut>(&mut self, name: &'static str, job: J, time: NaiveTime)
    where
        J: Fn(S, JobContext) -> Fut + Copy + Send + Sync + 'static,
        Fut: Future<Output = Result<(), Exception>> + Send + 'static,
    {
        self.add_job(name, job, Trigger::Daily { time_zone: self.timezone, time });
    }

    fn add_job<J, Fut>(&mut self, name: &'static str, job: J, trigger: Trigger)
    where
        J: Fn(S, JobContext) -> Fut + Copy + Send + Sync + 'static,
        Fut: Future<Output = Result<(), Exception>> + Send + 'static,
    {
        let job = Box::new(move |state: S, context| process_job(job, state, context));
        self.schedules.push(Arc::new(Schedule { name, job, trigger }));
    }

    pub async fn start(self, state: S, shutdown_signal: CancellationToken)
    where
        S: Clone,
    {
        assert!(!self.schedules.is_empty(), "scheduler does not have any jobs");

        let mut handles = JoinSet::new();
        for schedule in self.schedules {
            let state = state.clone();
            let shutdown_signal = shutdown_signal.clone();
            handles.spawn(async move {
                let mut previous = Utc::now();
                let mut first = true;
                loop {
                    let next = schedule.trigger.next(previous, first);
                    first = false;
                    let context = JobContext { name: schedule.name, scheduled_time: next };
                    let waiting_time = (context.scheduled_time - previous).to_std().unwrap_or(Duration::ZERO);
                    previous = context.scheduled_time;

                    let name = context.name;
                    let scheduled_time = context.scheduled_time.to_rfc3339_opts(SecondsFormat::Secs, true);
                    console!("job scheduled, name={name}, scheduled_time={scheduled_time}");

                    tokio::select! {
                        () = shutdown_signal.cancelled() => {
                            return;
                        }
                        () = time::sleep(waiting_time) => {
                            let state = state.clone();
                            task::__spawn(format!("job:{name}@{scheduled_time}"), (schedule.job)(state, context));
                        }
                    }
                }
            });
        }
        console!("scheduler started");
        handles.join_all().await;
        console!("scheduler stopped");
    }
}

fn process_job<S, J, Fut>(job: J, state: S, context: JobContext) -> Pin<Box<dyn Future<Output = ()> + Send>>
where
    S: Send + 'static,
    J: Fn(S, JobContext) -> Fut + Send + 'static,
    Fut: Future<Output = Result<(), Exception>> + Send + 'static,
{
    let ref_id = current_action_id().map(|id| vec![id]);
    let triggered = ref_id.is_some();
    Box::pin(
        log::start_action("job", ref_id, async move {
            context!(
                job = context.name,
                scheduled_time = context.scheduled_time.to_rfc3339_opts(SecondsFormat::Millis, true),
                fn = type_name::<J>()
            );
            if triggered {
                warn!(error_code = "MANUAL_OPERATION", "trigger job manually");
            }
            job(state, context).await
        })
        .map(drop), // start_action handled error with logging
    )
}
