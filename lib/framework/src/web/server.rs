use std::net::SocketAddr;
use std::sync::Arc;

use axum::Router;
use axum::extract::MatchedPath;
use axum::extract::Request;
use axum::http::StatusCode;
use axum::http::header;
use axum::middleware;
use axum::middleware::Next;
use axum::response::IntoResponse as _;
use axum::response::Response;
use axum_extra::extract::CookieJar;
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;
pub use tower_http::services::ServeDir;
pub use tower_http::services::ServeFile;
use tracing::debug;
use tracing::info;

use crate::exception::Exception;
use crate::log;
use crate::web::client_info::client_info;

pub struct HttpServerConfig {
    pub bind_address: String,
    pub max_forwarded_ips: usize,
}

impl Default for HttpServerConfig {
    fn default() -> Self {
        HttpServerConfig { bind_address: "0.0.0.0:8080".to_owned(), max_forwarded_ips: 2 }
    }
}

pub async fn start_http_server(
    router: Router,
    shutdown_signal: CancellationToken,
    config: HttpServerConfig,
) -> Result<(), Exception> {
    let app = Router::new();
    let app = app.merge(router);
    let app = app.layer(middleware::from_fn(http_server_layer));
    let app = app.into_make_service_with_connect_info::<SocketAddr>();
    let listener = TcpListener::bind(&config.bind_address).await?;
    info!("http server stated, bind={}", config.bind_address);
    axum::serve(listener, app)
        .with_graceful_shutdown(async move {
            shutdown_signal.cancelled().await;
        })
        .await?;
    info!("http server stopped");

    Ok(())
}

async fn http_server_layer(mut request: Request, next: Next) -> Response {
    // skip log for health check
    if request.uri().path() == "/health-check" {
        return StatusCode::OK.into_response(); // gce lb health check requires to return 200
    }

    let mut response = None;

    // TODO: get ref_id from header?

    log::start_action("http", None, async {
        context!(uri = request.uri().to_string(), method = request.method().as_str());

        for (name, value) in request.headers() {
            if name != header::COOKIE {
                debug!("[header] {name}={value:?}");
            }
        }
        let cookies = CookieJar::from_headers(request.headers());
        for cookie in cookies.iter() {
            debug!("[cookie] {}={}", cookie.name(), cookie.value());
        }

        let client_info = client_info(&request, 2);
        context!(client_ip = client_info.client_ip);
        if let Some(ref user_agent) = client_info.user_agent {
            context!(user_agent = user_agent);
        }
        request.extensions_mut().insert(Arc::new(client_info));

        let matched_path = request.extensions().get::<MatchedPath>().map(MatchedPath::as_str);
        if let Some(matched_path) = matched_path {
            context!(matched_path = matched_path);
        }

        if let Some(length) = request
            .headers()
            .get(header::CONTENT_LENGTH)
            .and_then(|v| v.to_str().ok())
            .and_then(|v| str::parse::<usize>(v).ok())
        {
            stats!(request_content_length = length);
        }

        let http_response = next.run(request).await;

        let status = http_response.status().as_u16();
        // TODO: warn on 404, 405?
        context!(response_status = status);
        for (name, value) in http_response.headers() {
            debug!("[header] {name}={value:?}");
        }
        response = Some(http_response);
        Ok(())
    })
    .await;
    if let Some(response) = response { response } else { StatusCode::INTERNAL_SERVER_ERROR.into_response() }
}
