use std::sync::Arc;

use axum::Router;
use chrono::NaiveDate;
use framework::exception::Exception;
use framework::task;
use framework_macro::api;
use serde::Deserialize;
use serde::Serialize;

use crate::AppState;
use crate::service::upload_archive;

pub fn routes(state: Arc<AppState>) -> Router<Arc<AppState>> {
    let service = OperationWebServiceImpl { state };
    operation_web_service::route(Arc::new(service))
}

#[api]
trait OperationWebService {
    #[put]
    #[path("/upload")]
    async fn upload(&self, request: UploadRequest) -> Result<(), Exception>;
}

#[derive(Serialize, Deserialize, Debug)]
struct UploadRequest {
    date: NaiveDate,
}

struct OperationWebServiceImpl {
    state: Arc<AppState>,
}

impl OperationWebService for OperationWebServiceImpl {
    async fn upload(&self, request: UploadRequest) -> Result<(), Exception> {
        let state = Arc::clone(&self.state);
        task::spawn_action("upload", async move { upload_archive(request.date, state).await });
        Ok(())
    }
}
