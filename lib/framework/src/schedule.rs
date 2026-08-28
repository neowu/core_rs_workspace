use std::any::type_name;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use futures::FutureExt as _;
use tokio::task::JoinSet;
use tokio::time;
use tokio_util::sync::CancellationToken;

use crate::exception::Exception;
use crate::log;
use crate::log::action;
use crate::schedule::trigger::Trigger;
use crate::task::TaskExecutor;
use crate::time::DateTime;
use crate::time::Offset;
use crate::time::SignedDuration;
use crate::time::Time;

pub mod controller;
mod trigger;

pub struct JobContext {
    pub name: &'static str,
    /// Scheduled time in the scheduler timezone.
    pub scheduled_time: DateTime,
}

type Job<S> = Box<dyn Fn(S, JobContext) -> Pin<Box<dyn Future<Output = ()> + Send>> + Send + Sync>;

struct Schedule<S> {
    name: &'static str,
    job: Job<S>,
    trigger: Trigger,
}

pub struct Scheduler<S> {
    timezone: Offset,
    schedules: Vec<Arc<Schedule<S>>>,
    executor: Arc<TaskExecutor>,
}

impl<S> Scheduler<S>
where
    S: Send + Sync + 'static,
{
    pub fn new(timezone: Offset) -> Self {
        Self { timezone, schedules: Vec::new(), executor: Arc::new(TaskExecutor::default()) }
    }

    pub fn schedule_fixed_rate<J, Fut>(&mut self, name: &'static str, job: J, interval: SignedDuration)
    where
        J: Fn(S, JobContext) -> Fut + Copy + Send + Sync + 'static,
        Fut: Future<Output = Result<(), Exception>> + Send + 'static,
    {
        assert!(interval.is_positive(), "interval must be positive, interval={interval:?}");
        self.add_job(name, job, Trigger::FixedRate(interval));
    }

    pub fn schedule_daily<J, Fut>(&mut self, name: &'static str, job: J, time: Time)
    where
        J: Fn(S, JobContext) -> Fut + Copy + Send + Sync + 'static,
        Fut: Future<Output = Result<(), Exception>> + Send + 'static,
    {
        self.add_job(name, job, Trigger::Daily(time));
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

        console!("start scheduler");

        let timezone = self.timezone;
        let mut handles = JoinSet::new();
        for schedule in self.schedules {
            let state = state.clone();
            let shutdown_signal = shutdown_signal.clone();
            let executor = Arc::clone(&self.executor);
            handles.spawn(async move {
                let mut previous = DateTime::now().with_timezone(timezone);
                let mut first = true;
                loop {
                    let next = schedule.trigger.next(previous, first);
                    first = false;
                    let context = JobContext { name: schedule.name, scheduled_time: next };
                    let waiting_time = context.scheduled_time - previous;
                    previous = context.scheduled_time;

                    let name = context.name;
                    let scheduled_time = context.scheduled_time.to_rfc3339();
                    console!("job scheduled, name={name}, scheduled_time={scheduled_time}");

                    tokio::select! {
                        () = shutdown_signal.cancelled() => {
                            return;
                        }
                        () = time::sleep(Duration::from_secs(waiting_time.as_secs() as u64)) => {
                            let state = state.clone();
                            executor.spawn(
                                format!("job:{name}@{scheduled_time}"),
                                (schedule.job)(state, context),
                            );
                        }
                    }
                }
            });
        }
        handles.join_all().await;
        if let Some(aborted) = self.executor.shutdown(Duration::from_secs(15)).await {
            console!("WARN job aborted, jobs={aborted:?}");
        }
        console!("scheduler stopped");
    }
}

fn process_job<S, J, Fut>(job: J, state: S, context: JobContext) -> Pin<Box<dyn Future<Output = ()> + Send>>
where
    S: Send + 'static,
    J: Fn(S, JobContext) -> Fut + Send + 'static,
    Fut: Future<Output = Result<(), Exception>> + Send + 'static,
{
    let ref_ids = log::current_action_id().map(|id| vec![id]);
    let triggered = ref_ids.is_some(); // must be triggered thru controller
    Box::pin(
        action("job", ref_ids, async move {
            context!(
                job = context.name,
                scheduled_time = context.scheduled_time.to_rfc3339(),
                fn = type_name::<J>()
            );
            if triggered {
                warn!(error_code = "MANUAL_OPERATION", "trigger job manually");
            }
            job(state, context).await
        })
        .map(drop),
    )
}
