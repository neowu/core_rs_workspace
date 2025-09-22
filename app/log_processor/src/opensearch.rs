use std::fmt::Debug;

use framework::exception;
use framework::exception::CoreRsResult;
use framework::http::HttpClient;
use framework::http::HttpMethod::POST;
use framework::http::HttpMethod::PUT;
use framework::http::HttpRequest;
use framework::json;
use serde::Serialize;

pub struct Opensearch {
    uri: String,
    client: HttpClient,
}

impl Opensearch {
    pub fn new(uri: &str) -> Self {
        Self {
            uri: uri.to_owned(),
            client: HttpClient::default(),
        }
    }

    pub async fn put_index_template(&self, name: &str, template: String) -> CoreRsResult<()> {
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

    pub async fn bulk_index<T>(&self, index: &str, documents: Vec<(String, T)>) -> CoreRsResult<()>
    where
        T: Serialize + Debug,
    {
        let uri = &self.uri;
        let mut request = HttpRequest::new(POST, format!("{uri}/_bulk"));

        let mut body = String::new();
        for (id, doc) in documents {
            body.push_str(&format!(r#"{{"index":{{"_index":"{index}","_id":"{id}"}}}}"#));
            body.push('\n');
            body.push_str(&json::to_json(&doc)?);
            body.push('\n');
        }

        request.body(body, "application/json");

        let response = self.client.execute(request).await?;
        if response.status != 200 {
            return Err(exception!(message = format!("failed to bulk index, index={index}")));
        }
        Ok(())
    }
}
