use std::fs;

use framework::exception::Exception;
use framework::web::router::Router;
use framework_macro::integration_test;
use http_test::TestServer;
use reqwest::StatusCode;
use reqwest::Version;
use reqwest::header;

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

    for client in [&server.http1, &server.h2c] {
        let response = client.get(server.url("/static/app.js")).send().await?;
        assert_eq!(response.status(), 200);
        assert_eq!(response.headers()[header::CONTENT_TYPE], "text/javascript; charset=utf-8");
        assert!(response.headers().get(header::LAST_MODIFIED).is_none());
        assert_eq!(response.text().await?, "console.log(1)");

        // conditional requests are ignored
        let response = client
            .get(server.url("/static/app.js"))
            .header(header::IF_MODIFIED_SINCE, "Thu, 01 Jan 2099 00:00:00 GMT")
            .header(header::IF_NONE_MATCH, "*")
            .send()
            .await?;
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.text().await?, "console.log(1)");

        assert_eq!(client.get(server.url("/static/")).send().await?.text().await?, "<h1>index</h1>");
        assert_eq!(client.get(server.url("/static/docs/")).send().await?.text().await?, "<h1>docs</h1>");
        assert_eq!(client.get(server.url("/favicon.ico")).send().await?.text().await?, "icon");

        let response = client.get(server.url("/static/empty.txt")).send().await?;
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.bytes().await?.len(), 0);

        let response = client.get(server.url("/static/large.bin")).send().await?;
        assert_eq!(response.headers()[header::CONTENT_LENGTH], large.len().to_string());
        assert_eq!(response.headers()[header::CONTENT_TYPE], "application/octet-stream");
        assert_eq!(response.bytes().await?, large);

        let response = client.head(server.url("/static/large.bin")).send().await?;
        assert_eq!(response.headers()[header::CONTENT_LENGTH], large.len().to_string());
        assert_eq!(response.bytes().await?.len(), 0);

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
            let response = client.get(server.url(path)).send().await?;
            assert_eq!(response.status(), StatusCode::NOT_FOUND, "path={path}");
        }

        let response = client.post(server.url("/static/app.js")).send().await?;
        assert_eq!(response.status(), StatusCode::METHOD_NOT_ALLOWED);
    }

    let response = server.h2c.get(server.url("/static/large.bin")).send().await?;
    assert_eq!(response.version(), Version::HTTP_2);
    assert_eq!(response.bytes().await?.len(), large.len());

    server.shutdown().await;
    fs::remove_dir_all(&root)?;
    Ok(())
}
