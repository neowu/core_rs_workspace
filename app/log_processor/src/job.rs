use std::sync::Arc;
use std::sync::LazyLock;

use chrono::NaiveDate;
use framework::exception::Exception;
use framework::schedule::JobContext;
use regex::Regex;

use crate::AppState;
use crate::elasticsearch::IndexState;

pub async fn cleanup_old_index_job(state: Arc<AppState>, context: JobContext) -> Result<(), Exception> {
    let today = context.scheduled_time.date_naive();
    let cluster_state = state.elasticsearch.state().await?;
    for (name, index) in cluster_state.metadata.indices {
        if let Some(date) = created_date(&name) {
            let days = (today - date).num_days();
            if days > 30 {
                state.elasticsearch.delete_index(name).await?;
            } else if days > 7 && matches!(index.state, IndexState::Open) {
                state.elasticsearch.close_index(name).await?;
            }
        }
    }
    Ok(())
}

fn created_date(index: &str) -> Option<NaiveDate> {
    static INDEX_REGEX: LazyLock<Regex> =
        LazyLock::new(|| Regex::new(r#"^\w[\w.\-]+-(\d{4}\.\d{2}\.\d{2})$"#).unwrap());

    if let Some(captures) = INDEX_REGEX.captures(index) {
        let date = captures[1].to_string();
        if let Ok(date) = NaiveDate::parse_from_str(&date, "%Y.%m.%d") {
            return Some(date);
        }
    }

    None
}

#[cfg(test)]
mod tests {
    use chrono::NaiveDate;

    #[test]
    fn created_date() {
        assert_eq!(
            super::created_date("action-2025.11.05"),
            NaiveDate::from_ymd_opt(2025, 11, 5)
        );
        assert_eq!(
            super::created_date(".ds-.edr-workflow-insights-default-2025.04.24-000001"),
            None
        );
        assert_eq!(super::created_date(".kibana-2025.04.25"), None);
    }
}
