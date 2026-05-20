use std::collections::HashMap;
use std::fmt::Debug;

use framework::exception;
use framework::exception::Exception;
use framework::http::HttpClient;
use framework::http::HttpClientConfig;
use framework::http::HttpMethod::Delete;
use framework::http::HttpMethod::Get;
use framework::http::HttpMethod::Post;
use framework::http::HttpMethod::Put;
use framework::http::HttpRequest;
use framework::json;
use framework::stats;
use framework::write_str;
use serde::Deserialize;
use serde::Serialize;
use tracing::Instrument as _;
use tracing::debug;
use tracing::debug_span;

pub(crate) struct Elasticsearch {
    uri: String,
    client: HttpClient,
}

impl Elasticsearch {
    pub(crate) fn new(uri: String) -> Self {
        Self { uri, client: HttpClient::new(HttpClientConfig::default()) }
    }

    pub(crate) async fn put_index_template(&self, name: &str, template: String) -> Result<(), Exception> {
        let span = debug_span!("es");
        async {
            debug!(name, "put index template");
            let uri = &self.uri;
            let mut request = HttpRequest::new(Put, format!("{uri}/_index_template/{name}"));
            request.body(template, "application/json");
            let response = self.client.execute(request).await?;
            if response.status != 200 {
                return Err(exception!(format!("failed to create index template, name={name}")));
            }
            Ok(())
        }
        .instrument(span)
        .await
    }

    pub(crate) async fn bulk_index<T>(&self, index: &str, documents: Vec<(String, T)>) -> Result<(), Exception>
    where
        T: Serialize + Debug,
    {
        let span = debug_span!("es", index);
        async {
            debug!(index, "bulk index");
            let uri = &self.uri;
            let mut request = HttpRequest::new(Post, format!("{uri}/_bulk"));

            let mut body = String::new();
            for (id, doc) in &documents {
                write_str!(body, r#"{{"index":{{"_index":"{index}","_id":"{id}"}}}}"#);
                body.push('\n');
                body.push_str(&json::to_json(&doc)?);
                body.push('\n');
            }
            stats!(es_write_entries = documents.len(), es_write_bytes = body.len());
            request.body(body, "application/json");

            let response = self.client.execute(request).await?;
            if response.status != 200 {
                return Err(exception!(format!("failed to bulk index, index={index}")));
            }
            Ok(())
        }
        .instrument(span)
        .await
    }

    pub(crate) async fn state(&self) -> Result<ClusterStateResponse, Exception> {
        let span = debug_span!("es");
        async {
            let uri = &self.uri;
            let request = HttpRequest::new(Get, format!("{uri}/_cluster/state"));
            let response = self.client.execute(request).await?;
            if response.status != 200 {
                return Err(exception!(format!("failed to get state")));
            }
            json::from_json(&response.body)
        }
        .instrument(span)
        .await
    }

    pub(crate) async fn close_index(&self, index: String) -> Result<(), Exception> {
        let span = debug_span!("es");
        async {
            debug!(index, "close index");
            let uri = &self.uri;
            let request = HttpRequest::new(Post, format!("{uri}/{index}/_close"));
            let response = self.client.execute(request).await?;
            if response.status != 200 {
                return Err(exception!(format!("failed to close index, index={index}")));
            }
            Ok(())
        }
        .instrument(span)
        .await
    }

    pub(crate) async fn delete_index(&self, index: String) -> Result<(), Exception> {
        let span = debug_span!("es");
        async {
            debug!(index, "delete index");
            let uri = &self.uri;
            let request = HttpRequest::new(Delete, format!("{uri}/{index}"));
            let response = self.client.execute(request).await?;
            if response.status != 200 {
                return Err(exception!(format!("failed to delete index, index={index}")));
            }
            Ok(())
        }
        .instrument(span)
        .await
    }
}

#[derive(Debug, Deserialize)]
pub(crate) struct ClusterStateResponse {
    pub metadata: Metadata,
}

#[derive(Debug, Deserialize)]
pub(crate) struct Metadata {
    pub indices: HashMap<String, Index>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct Index {
    pub state: IndexState,
}

#[derive(Debug, Deserialize)]
pub(crate) enum IndexState {
    #[serde(rename = "open")]
    Open,
    #[serde(rename = "close")]
    Close,
}
