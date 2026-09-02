use framework::exception;
use framework::exception::Exception;
use framework::http::HttpClient;
use framework::http::HttpClientConfig;
use framework::http::HttpRequest;
use framework::http::Method;
use framework::http::header;
use framework::json;
use framework::log::Severity;
use framework::span;
use serde::Deserialize;
use serde::Serialize;

pub(crate) struct SlackClient {
    token: String,
    error_channel: String,
    warn_channel: String,
    client: HttpClient,
}

#[derive(Debug, Serialize)]
struct SlackMessageApiRequest<'a> {
    channel: &'a str,
    attachments: [Attachment<'a>; 1],
}

#[derive(Debug, Serialize)]
struct Attachment<'a> {
    text: &'a str,
    color: &'static str,
}

#[derive(Debug, Deserialize)]
struct SlackMessageApiResponse {
    ok: bool,
}

impl SlackClient {
    pub(crate) fn new(token: String, error_channel: String, warn_channel: String) -> Self {
        Self { token, error_channel, warn_channel, client: HttpClient::new(HttpClientConfig::default()) }
    }

    // refer to https://api.slack.com/methods/chat.postMessage
    pub(crate) async fn send(&self, severity: Severity, message: &str) -> Result<(), Exception> {
        let _span = span!("slack");

        let color = if severity == Severity::Error { "#a30101" } else { "#ff5c33" };

        let body = json::to_json(&SlackMessageApiRequest {
            channel: self.channel(severity),
            attachments: [Attachment { color, text: message }],
        })?;

        let mut request = HttpRequest::new(Method::POST, "https://slack.com/api/chat.postMessage");
        request.header(header::AUTHORIZATION, &format!("Bearer {}", self.token))?;
        request.body(body, "application/json");

        let response = self.client.execute(request).await?;
        if response.status != 200 {
            return Err(exception!(format!(
                "failed to send message to slack, status={}, response={}",
                response.status, response.body
            )));
        }

        // slack answers 200 with ok=false on business errors, e.g. invalid_auth or channel_not_found
        let result: SlackMessageApiResponse = json::from_json(&response.body)?;
        if !result.ok {
            return Err(exception!(format!("failed to send message to slack, response={}", response.body)));
        }

        Ok(())
    }

    // INFO never alerts, so anything but ERROR belongs to the warn channel
    fn channel(&self, severity: Severity) -> &str {
        if severity == Severity::Error { &self.error_channel } else { &self.warn_channel }
    }
}

#[cfg(test)]
mod tests {
    use framework::log::Severity;

    use super::SlackClient;

    #[test]
    fn channel_per_severity() {
        let client = SlackClient::new("token".to_owned(), "#error".to_owned(), "#warn".to_owned());

        assert_eq!(client.channel(Severity::Error), "#error");
        assert_eq!(client.channel(Severity::Warn), "#warn");
    }
}
