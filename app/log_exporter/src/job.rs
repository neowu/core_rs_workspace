use std::sync::Arc;

use chrono::Days;
use framework::exception::CoreRsResult;
use framework::schedule::JobContext;

use crate::AppState;
use crate::service::cleanup_archive;
use crate::service::upload_archive;

pub async fn process_log_job(state: Arc<AppState>, context: JobContext) -> CoreRsResult<()> {
    let today = context.scheduled_time.date_naive();
    cleanup_archive(today.checked_sub_days(Days::new(5)).unwrap(), &state)?;
    upload_archive(today, &state).await?;
    Ok(())
}
