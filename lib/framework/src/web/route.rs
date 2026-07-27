use std::any::type_name_of_val;
use std::convert::Infallible;

use axum::extract::Request;
use axum::handler::Handler;
use axum::routing;
pub use axum::routing::MethodFilter;
pub use axum::routing::MethodRouter;

#[derive(Clone)]
struct Controller<H> {
    name: &'static str,
    handler: H,
}

impl<H, T, S> Handler<T, S> for Controller<H>
where
    H: Handler<T, S>,
    S: Send + Sync + 'static,
{
    type Future = H::Future;

    // called within http server layer action scope, so context is attached to current action
    #[inline]
    fn call(self, request: Request, state: S) -> Self::Future {
        context!(fn = self.name);
        self.handler.call(request, state)
    }
}

macro_rules! method_fn {
    ($($name:ident),+ $(,)?) => {
        $(
            #[inline]
            pub fn $name<H, T, S>(handler: H) -> MethodRouter<S, Infallible>
            where
                H: Handler<T, S>,
                T: 'static,
                S: Clone + Send + Sync + 'static,
            {
                let name = type_name_of_val(&handler);
                routing::$name(Controller { handler, name })
            }
        )+
    };
}

method_fn!(get, post, put, delete, patch, head, options, trace);

/// routes requests with the given method filter to the given handler, with `fn` context logging.
#[inline]
pub fn on<H, T, S>(filter: MethodFilter, handler: H) -> MethodRouter<S, Infallible>
where
    H: Handler<T, S>,
    T: 'static,
    S: Clone + Send + Sync + 'static,
{
    let name = type_name_of_val(&handler);
    routing::on(filter, Controller { name, handler })
}

#[cfg(test)]
mod tests {
    use std::any::type_name_of_val;

    use axum::Router;
    use axum::body::Body;
    use axum::handler::Handler as _;
    use axum::http::StatusCode;

    use super::Controller;
    use super::Request;
    use super::get;

    async fn hello() -> &'static str {
        "ok"
    }

    #[test]
    fn handler_name() {
        assert_eq!(type_name_of_val(&hello), "framework::web::route::tests::hello");
    }

    #[tokio::test]
    async fn call_handler() {
        let handler = Controller { handler: hello, name: "framework::web::route::tests::hello" };
        let response = handler.call(Request::new(Body::empty()), ()).await;
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[test]
    fn build_router() {
        let _router: Router = Router::new().route("/hello", get(hello));
    }
}
