use framework::exception;
use framework::exception::Exception;
use framework::http::HeaderName;
use framework::http::HttpClient;
use framework::http::HttpClientConfig;
use framework::http::HttpRequest;
use framework::http::Method;

pub(crate) async fn import(kibana_uri: &str, objects: String) -> Result<(), Exception> {
    let http_client = HttpClient::new(HttpClientConfig::default());
    let mut request =
        HttpRequest::new(Method::POST, format!("{kibana_uri}/api/saved_objects/_bulk_create?overwrite=true"));
    request.header(HeaderName::from_static("kbn-xsrf"), "true")?;
    // request.headers.insert(HeaderName::from_static("osd-xsrf"), "true".to_string());
    request.body(objects, "application/json");

    let response = http_client.execute(request).await?;
    if response.status == 200 {
        Ok(())
    } else {
        Err(exception!(format!("failed to import kibana objects, status={}", response.status)))
    }
}
