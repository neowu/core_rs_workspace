use framework::exception::Exception;
use framework::schedule::JobContext;

use crate::AppState;

pub(crate) async fn demo_job(_state: &AppState, context: JobContext) -> Result<(), Exception> {
    println!("run demo job, scheduled_time={}", context.scheduled_time);
    Ok(())
}
