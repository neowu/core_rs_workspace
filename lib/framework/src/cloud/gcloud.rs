use std::env;
use std::time::Duration;

use crate::exception::Exception;
use crate::http::HeaderName;
use crate::http::HttpClient;
use crate::http::HttpClientConfig;
use crate::http::HttpRequest;
use crate::http::Method;
use crate::network::hostname;
use crate::system::Env;

/// Builds the host name on gcloud run, where the os hostname is always "localhost".
pub struct CloudRunEnv;

impl Env for CloudRunEnv {
    async fn host(&self) -> String {
        // CLOUD_RUN_EXECUTION is only set on job, worker pool falls back to the revision
        let Ok(revision) = env::var("CLOUD_RUN_EXECUTION").or_else(|_| env::var("CLOUD_RUN_REVISION")) else {
            console!("WARN not found cloud run env, fallback to hostname");
            return hostname();
        };

        match instance_id().await {
            Ok(id) => {
                let id = id.trim();
                let host = format!("{revision}-{}", id.get(..8).unwrap_or(id));
                console!("found cloud run env, host={host}");
                host
            }
            Err(err) => {
                console!("WARN failed to query gcloud metadata server, only use revision, error={err}");
                revision
            }
        }
    }
}

async fn instance_id() -> Result<String, Exception> {
    let client = HttpClient::new(HttpClientConfig { timeout: Duration::from_secs(3), ..HttpClientConfig::default() });

    let mut request = HttpRequest::new(Method::GET, "http://metadata.google.internal/computeMetadata/v1/instance/id");
    request.header(HeaderName::from_static("metadata-flavor"), "Google")?;

    let response = client.execute(request).await?;
    if response.status != 200 {
        return Err(exception!(format!("failed to get instance id, status={}", response.status)));
    }
    Ok(response.body)
}
