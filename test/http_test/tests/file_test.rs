use std::fs;

use framework::exception::Exception;
use framework::http::HttpRequest;
use framework::http::Method;
use framework::http::header;
use framework::web::router::Router;
use framework_macro::integration_test;
use http_test::TestServer;
use http_test::execute;
use http_test::http_client;

#[integration_test]
async fn static_files() -> Result<(), Exception> {
    let root = std::env::temp_dir().join(format!("http_test_{}", std::process::id()));
    fs::create_dir_all(root.join("www/docs"))?;
    fs::write(root.join("www/index.html"), "<h1>index</h1>")?;
    fs::write(root.join("www/docs/index.html"), "<h1>docs</h1>")?;
    fs::write(root.join("www/app.js"), "console.log(1)")?;
    fs::write(root.join("www/.env"), "secret")?;
    fs::write(root.join("www/empty.txt"), "")?;
    let large: Vec<u8> = (0..3 * 1024 * 1024 + 7).map(|i| (i % 251) as u8).collect();
    fs::write(root.join("www/large.bin"), &large)?;
    fs::write(root.join("secret.txt"), "secret")?;
    fs::write(root.join("favicon.ico"), "icon")?;

    let router = Router::new().dir("/static/", root.join("www")).file("/favicon.ico", root.join("favicon.ico"));
    let mut server = TestServer::start(router).await;

    for h2c in [false, true] {
        let client = &http_client(h2c);
        let response = execute(client, Method::GET, server.url("/static/app.js")).await?;
        assert_eq!(response.status, 200);
        assert_eq!(response.headers[header::CONTENT_TYPE], "text/javascript; charset=utf-8");
        assert!(response.headers.get(header::LAST_MODIFIED).is_none());
        assert_eq!(response.text()?, "console.log(1)");

        // conditional requests are ignored
        let mut request = HttpRequest::new(Method::GET, server.url("/static/app.js"))?;
        request.header(header::IF_MODIFIED_SINCE, "Thu, 01 Jan 2099 00:00:00 GMT")?;
        request.header(header::IF_NONE_MATCH, "*")?;
        let response = client.execute(request).await?;
        assert_eq!(response.status, 200);
        assert_eq!(response.text()?, "console.log(1)");

        assert_eq!(execute(client, Method::GET, server.url("/static/")).await?.text()?, "<h1>index</h1>");
        assert_eq!(execute(client, Method::GET, server.url("/static/docs/")).await?.text()?, "<h1>docs</h1>");
        assert_eq!(execute(client, Method::GET, server.url("/favicon.ico")).await?.text()?, "icon");

        let response = execute(client, Method::GET, server.url("/static/empty.txt")).await?;
        assert_eq!(response.status, 200);
        assert!(response.body.is_empty());

        let response = execute(client, Method::GET, server.url("/static/large.bin")).await?;
        assert_eq!(response.headers[header::CONTENT_LENGTH], large.len().to_string());
        assert_eq!(response.headers[header::CONTENT_TYPE], "application/octet-stream");
        assert_eq!(response.body, large);

        let response = execute(client, Method::HEAD, server.url("/static/large.bin")).await?;
        assert_eq!(response.headers[header::CONTENT_LENGTH], large.len().to_string());
        assert!(response.body.is_empty());

        for path in [
            "/static/missing.js",
            "/static/docs",
            "/static/.env",
            "/static/%2Eenv",
            "/static/%2E%2E/secret.txt",
            "/static/..%2Fsecret.txt",
            "/static/.%2Fapp.js",
            "/static/docs%2F.%2Findex.html",
            "/static/docs%2F%2Findex.html",
        ] {
            let response = execute(client, Method::GET, server.url(path)).await?;
            assert_eq!(response.status, 404, "path={path}");
        }

        let response = execute(client, Method::POST, server.url("/static/app.js")).await?;
        assert_eq!(response.status, 405);
    }

    server.shutdown().await;
    fs::remove_dir_all(&root)?;
    Ok(())
}
