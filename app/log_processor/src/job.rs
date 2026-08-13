use std::sync::Arc;
use std::sync::LazyLock;

use framework::date::Date;
use framework::exception::Exception;
use framework::schedule::JobContext;
use regex::Regex;

use crate::AppState;
use crate::elasticsearch::IndexState;

pub(crate) async fn cleanup_old_index_job(state: Arc<AppState>, context: JobContext) -> Result<(), Exception> {
    let today = context.scheduled_time.date();
    let cluster_state = state.elasticsearch.state().await?;
    for (name, index) in cluster_state.metadata.indices {
        if let Some(date) = created_date(&name) {
            let days = today - date;
            if days > 30 {
                state.elasticsearch.delete_index(name).await?;
            } else if days > 7 && matches!(index.state, IndexState::Open) {
                state.elasticsearch.close_index(name).await?;
            }
        }
    }
    Ok(())
}

fn created_date(index: &str) -> Option<Date> {
    static INDEX_REGEX: LazyLock<Regex> =
        LazyLock::new(|| Regex::new(r"^\w[\w.\-]+-(\d{4}\.\d{2}\.\d{2})$").expect("value must be valid"));

    if let Some(captures) = INDEX_REGEX.captures(index) {
        let date = captures[1].replace('.', "-");
        if let Ok(date) = Date::parse(&date) {
            return Some(date);
        }
    }

    None
}

pub(crate) async fn archive_to_gcs_job(state: Arc<AppState>, context: JobContext) -> Result<(), Exception> {
    let yesterday = Date::add_days(context.scheduled_time.date(), -1)?;
    let (year, _, _) = yesterday.to_ymd();
    let date = yesterday.to_rfc3339();
    if let Some(clickhouse) = &state.clickhouse {
        clickhouse
            .execute(
                "INSERT INTO FUNCTION gcs(gcs_archive, filename = ?) SELECT * FROM log.action WHERE toDate(timestamp) = ?",
                &[&format!("log/action/{year}/action-{date}.parquet"), &yesterday],
            )
            .await?;

        clickhouse
            .execute(
                "INSERT INTO FUNCTION gcs(gcs_archive, filename = ?) SELECT * FROM log.event WHERE toDate(timestamp) = ?",
                &[&format!("log/event/{year}/event-{date}.parquet"), &yesterday],
            )
            .await?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use framework::date::Date;

    #[test]
    fn created_date() {
        assert_eq!(super::created_date("action-2025.11.05"), Some(Date::new(2025, 11, 5)));
        assert_eq!(super::created_date(".ds-.edr-workflow-insights-default-2025.04.24-000001"), None);
        assert_eq!(super::created_date(".kibana-2025.04.25"), None);
    }
}
