use std::fs;
use std::path::PathBuf;
use std::sync::Arc;

use chrono::Datelike;
use chrono::NaiveDate;
use framework::exception::CoreRsResult;
use framework::shell;
use tracing::info;

use crate::AppState;

pub fn local_file_path(name: &str, date: NaiveDate, state: &Arc<AppState>) -> CoreRsResult<PathBuf> {
    let dir = &state.log_dir;
    let year = date.year();
    let hash = &state.hash;
    let path = PathBuf::from(format!("{dir}/{name}/{year}/{name}-{date}-{hash}.ndjson"));
    if let Some(parent) = path.parent()
        && !parent.exists()
    {
        fs::create_dir_all(parent)?;
    }
    Ok(path)
}

pub fn cleanup_archive(date: NaiveDate, state: &Arc<AppState>) -> CoreRsResult<()> {
    info!("clean up archives, date={date}");

    let action_log_path = local_file_path("action", date, state)?;
    if action_log_path.exists() {
        fs::remove_file(&action_log_path)?;
    }

    let event_path = local_file_path("event", date, state)?;
    if event_path.exists() {
        fs::remove_file(&event_path)?;
    }

    Ok(())
}

pub async fn upload_archive(date: NaiveDate, state: &Arc<AppState>) -> CoreRsResult<()> {
    let action_log_path = local_file_path("action", date, state)?;
    if action_log_path.exists() {
        let remote_path = remote_path("action", date, state);
        let columns = "{'date': 'TIMESTAMPTZ', id: 'STRING', app: 'STRING', host: 'STRING', result: 'STRING', action: 'STRING', ref_ids: 'STRING[]', correlation_ids: 'STRING[]', clients: 'STRING[]', error_code: 'STRING', error_message: 'STRING', elapsed: 'LONG', context: 'MAP(STRING, STRING[])', stats: 'MAP(STRING, DOUBLE)', perf_stats: 'MAP(STRING, MAP(STRING, DOUBLE))'}";
        convert_parquet_and_upload(action_log_path, &remote_path, columns).await?;
    }

    let event_path = local_file_path("event", date, state)?;
    if event_path.exists() {
        let remote_path = remote_path("event", date, state);
        let columns = "{'date': 'TIMESTAMPTZ', id: 'STRING', app: 'STRING', received_time: 'TIMESTAMPTZ', result: 'STRING', action: 'STRING', error_code: 'STRING', error_message: 'STRING', elapsed: 'LONG', context: 'MAP(STRING, STRING)', stats: 'MAP(STRING, DOUBLE)', info: 'MAP(STRING, STRING)'}";
        convert_parquet_and_upload(event_path, &remote_path, columns).await?;
    }

    Ok(())
}

async fn convert_parquet_and_upload(local_path_buf: PathBuf, remote_path: &str, columns: &str) -> CoreRsResult<()> {
    let local_path = local_path_buf.to_string_lossy();
    info!("convert to parquet, path={local_path}");
    let parquet_path_buf = local_path_buf.with_extension("parquet");
    let parquet_path = parquet_path_buf.to_string_lossy();
    let command = format!(
        r#"SET memory_limit='256MB';SET temp_directory='/tmp/duckdb';COPY (SELECT * FROM read_ndjson(['{local_path}'], columns = {columns})) TO '{parquet_path}' (FORMAT 'parquet');"#
    );
    shell::run(&format!("duckdb -c \"{command}\"")).await?;

    info!("upload archive, path={parquet_path}");
    let command = format!("gcloud storage cp --quiet {parquet_path} {remote_path}");
    shell::run(&command).await?;
    fs::remove_file(parquet_path_buf)?;
    Ok(())
}

fn remote_path(name: &str, date: NaiveDate, state: &Arc<AppState>) -> String {
    let bucket = &state.bucket;
    let year = date.year();
    let hash = &state.hash;
    format!("{bucket}/{name}/{year}/{name}-{date}-{hash}.parquet")
}
