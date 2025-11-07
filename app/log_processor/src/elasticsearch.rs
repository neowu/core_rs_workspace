use std::collections::HashMap;
use std::fmt::Debug;

use framework::exception;
use framework::exception::Exception;
use framework::http::HttpClient;
use framework::http::HttpMethod::DELETE;
use framework::http::HttpMethod::POST;
use framework::http::HttpMethod::PUT;
use framework::http::HttpRequest;
use framework::json;
use serde::Deserialize;
use serde::Serialize;
use tracing::Instrument;
use tracing::debug;
use tracing::debug_span;

pub struct Elasticsearch {
    uri: String,
    client: HttpClient,
}

impl Elasticsearch {
    pub fn new(uri: &str) -> Self {
        Self {
            uri: uri.to_owned(),
            client: HttpClient::default(),
        }
    }

    pub async fn put_index_template(&self, name: &str, template: String) -> Result<(), Exception> {
        let span = debug_span!("es");
        async {
            debug!(name, "put index tempalte");
            let uri = &self.uri;
            let mut request = HttpRequest::new(PUT, format!("{uri}/_index_template/{name}"));
            request.body(template, "application/json");
            let response = self.client.execute(request).await?;
            if response.status != 200 {
                return Err(exception!(
                    message = format!("failed to create index template, name={name}")
                ));
            }
            Ok(())
        }
        .instrument(span)
        .await
    }

    pub async fn bulk_index<T>(&self, index: &str, documents: Vec<(String, T)>) -> Result<(), Exception>
    where
        T: Serialize + Debug,
    {
        let span = debug_span!("es", index);
        async {
            debug!(index, "bulk index");
            let uri = &self.uri;
            let mut request = HttpRequest::new(POST, format!("{uri}/_bulk"));

            let mut body = String::new();
            for (id, doc) in documents.iter() {
                body.push_str(&format!(r#"{{"index":{{"_index":"{index}","_id":"{id}"}}}}"#));
                body.push('\n');
                body.push_str(&json::to_json(doc)?);
                body.push('\n');
            }
            debug!(es_write_entries = documents.len(), es_write_bytes = body.len(), "stats");
            request.body(body, "application/json");

            let response = self.client.execute(request).await?;
            if response.status != 200 {
                return Err(exception!(message = format!("failed to bulk index, index={index}")));
            }
            Ok(())
        }
        .instrument(span)
        .await
    }

    pub async fn state(&self) -> Result<ClusterStateResponse, Exception> {
        let span = debug_span!("es");
        async {
            let uri = &self.uri;
            let request = HttpRequest::new(PUT, format!("{uri}/_cluster/state"));
            let response = self.client.execute(request).await?;
            if response.status != 200 {
                return Err(exception!(message = format!("failed to get state")));
            }
            json::from_json(&response.body)
        }
        .instrument(span)
        .await
    }

    pub async fn close_index(&self, index: String) -> Result<(), Exception> {
        let span = debug_span!("es");
        async {
            debug!(index, "close index");
            let uri = &self.uri;
            let request = HttpRequest::new(POST, format!("{uri}/{index}/_close"));
            let response = self.client.execute(request).await?;
            if response.status != 200 {
                return Err(exception!(message = format!("failed to close index, index={index}")));
            }
            Ok(())
        }
        .instrument(span)
        .await
    }

    pub async fn delete_index(&self, index: String) -> Result<(), Exception> {
        let span = debug_span!("es");
        async {
            debug!(index, "delete index");
            let uri = &self.uri;
            let request = HttpRequest::new(DELETE, format!("{uri}/{index}"));
            let response = self.client.execute(request).await?;
            if response.status != 200 {
                return Err(exception!(message = format!("failed to delete index, index={index}")));
            }
            Ok(())
        }
        .instrument(span)
        .await
    }
}

#[derive(Debug, Deserialize)]
pub struct ClusterStateResponse {
    pub metadata: Metadata,
}

#[derive(Debug, Deserialize)]
pub struct Metadata {
    pub indices: HashMap<String, Index>,
}

#[derive(Debug, Deserialize)]
pub struct Index {
    pub state: IndexState,
}

#[derive(Debug, Deserialize)]
pub enum IndexState {
    #[serde(rename = "open")]
    Open,
    #[serde(rename = "close")]
    Close,
}
