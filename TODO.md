* write claude.md better? show how to do things, e.g. integration steps
* refactor shutdown with CancellationToken

* axum https?
* action log records message_handler, job_handler, task_handler? only by macro with file!()/line!()/column!()?
* make state with Box::leak()

* log_collector supports collect cookies

* impl SSE client?
```
pub struct HttpStreamResponse {
    response: reqwest::Response,
}

type BytesResult = Result<Bytes, reqwest::Error>;
impl HttpStreamResponse {
    pub fn lines(
        self,
    ) -> Lines<IntoAsyncRead<MapErr<impl Stream<Item = BytesResult>, impl FnMut(reqwest::Error) -> io::Error>>> {
        self.response
            .bytes_stream()
            .map_err(io::Error::other)
            .into_async_read()
            .lines()
    }

    pub async fn text(self) -> Result<String, Exception> {
        let body = self.response.text().await?;

        Ok(body)
    }

    pub fn status_code(&self) -> u16 {
        self.response.status().as_u16()
    }
}
```
