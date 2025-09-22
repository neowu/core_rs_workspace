use std::pin::Pin;
use std::time::Duration;

use chrono::DateTime;
use chrono::FixedOffset;
use chrono::NaiveTime;
use chrono::SecondsFormat;
use chrono::Utc;
use tokio::sync::broadcast;
use tokio::time;
use tracing::debug;
use tracing::info;
use trigger::DailyTrigger;
use trigger::FixedRateTrigger;

use crate::exception::CoreRsResult;
use crate::log;

mod trigger;

pub struct JobContext {
    pub name: &'static str,
    pub scheduled_time: DateTime<Utc>,
}

trait Job<S>: Send {
    fn execute(&self, state: S, context: JobContext) -> Pin<Box<dyn Future<Output = ()> + Send>>;
}

impl<F, Fut, S> Job<S> for F
where
    F: Fn(S, JobContext) -> Fut + Send,
    Fut: Future<Output = ()> + Send + 'static,
{
    fn execute(&self, state: S, context: JobContext) -> Pin<Box<dyn Future<Output = ()> + Send>> {
        Box::pin(self(state, context))
    }
}

trait Trigger: Send {
    fn next(&self, previous: DateTime<Utc>) -> DateTime<Utc>;
}

struct Schedule<S> {
    name: &'static str,
    job: Box<dyn Job<S>>,
    trigger: Box<dyn Trigger>,
}

pub struct Scheduler<S> {
    timezone: FixedOffset,
    schedules: Vec<Schedule<S>>,
}

impl<S> Scheduler<S>
where
    S: Send + Sync + 'static,
{
    pub fn new(timezone: FixedOffset) -> Self {
        Self {
            timezone,
            schedules: Vec::new(),
        }
    }

    pub fn schedule_fixed_rate<J, Fut>(&mut self, name: &'static str, job: J, interval: Duration)
    where
        J: Fn(S, JobContext) -> Fut + Copy + Send + 'static,
        Fut: Future<Output = CoreRsResult<()>> + Send + 'static,
    {
        let trigger = Box::new(FixedRateTrigger { interval });
        self.add_job(name, job, trigger);
    }

    pub fn schedule_daily<J, Fut>(&mut self, name: &'static str, job: J, time: NaiveTime)
    where
        J: Fn(S, JobContext) -> Fut + Copy + Send + 'static,
        Fut: Future<Output = CoreRsResult<()>> + Send + 'static,
    {
        let trigger = Box::new(DailyTrigger {
            time_zone: self.timezone,
            time,
        });
        self.add_job(name, job, trigger);
    }

    fn add_job<J, Fut>(&mut self, name: &'static str, job: J, trigger: Box<dyn Trigger>)
    where
        J: Fn(S, JobContext) -> Fut + Copy + Send + 'static,
        Fut: Future<Output = CoreRsResult<()>> + Send + 'static,
    {
        let job = move |state: S, context| process_job(job, state, context);
        self.schedules.push(Schedule {
            name,
            job: Box::new(job),
            trigger,
        });
    }

    pub async fn start(self, state: S, shutdown_signel: broadcast::Receiver<()>) -> CoreRsResult<()>
    where
        S: Clone,
    {
        let mut handles = vec![];
        for schedule in self.schedules {
            let state = state.clone();
            let mut shutdown_signel = shutdown_signel.resubscribe();
            handles.push(tokio::spawn(async move {
                time::sleep(Duration::from_secs(3)).await; // initial delay
                let mut previous = Utc::now();
                loop {
                    let next = schedule.trigger.next(previous);
                    let context = JobContext {
                        name: schedule.name,
                        scheduled_time: next,
                    };
                    info!(
                        name = context.name,
                        scheduled_time = context.scheduled_time.to_rfc3339_opts(SecondsFormat::Millis, true),
                        "scheduled job"
                    );
                    let waiting_time = (context.scheduled_time - previous).to_std().unwrap();
                    previous = context.scheduled_time;
                    tokio::select! {
                        _ = shutdown_signel.recv() => {
                            return;
                        }
                        _ = time::sleep(waiting_time) => {
                            let state = state.clone();
                            tokio::spawn(schedule.job.execute(state, context));
                        }
                    }
                }
            }));
        }
        info!("scheduler started");
        for handle in handles {
            handle.await.unwrap();
        }
        info!("scheduler stopped");
        Ok(())
    }
}

async fn process_job<S, J, Fut>(job: J, state: S, context: JobContext)
where
    J: Fn(S, JobContext) -> Fut,
    Fut: Future<Output = CoreRsResult<()>>,
{
    log::start_action("job", None, async move {
        let name = context.name;
        let scheduled_time = context.scheduled_time.to_rfc3339_opts(SecondsFormat::Millis, true);
        debug!(name, "[job]");
        debug!(scheduled_time, "[job]");
        debug!(job = name, scheduled_time, "context");
        job(state, context).await
    })
    .await
}
