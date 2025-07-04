use std::sync::Arc;

use anyhow::Result;
use chrono::Days;
use core_ng::schedule::JobContext;

use crate::AppState;
use crate::service::cleanup_archive;
use crate::service::upload_archive;

pub async fn process_log_job(state: Arc<AppState>, context: JobContext) -> Result<()> {
    let today = context.scheduled_time.date_naive();
    cleanup_archive(today.checked_sub_days(Days::new(5)).unwrap(), &state)?;
    upload_archive(today, &state).await?;
    Ok(())
}
