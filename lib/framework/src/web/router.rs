use std::any::type_name_of_val;
use std::cmp::Reverse;
use std::collections::HashMap;
use std::path::Path;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;

use http::Method;

use crate::exception::Exception;
use crate::web::file;
use crate::web::request::Request;
use crate::web::response::Response;

pub(crate) type HandlerFuture = Pin<Box<dyn Future<Output = Result<Response, Exception>> + Send>>;
type Handler = Box<dyn Fn(Request) -> HandlerFuture + Send + Sync>;
type SharedHandler = Arc<dyn Fn(Request) -> HandlerFuture + Send + Sync>;
type Interceptor<S> = Arc<dyn Fn(Arc<S>, Request, Next) -> HandlerFuture + Send + Sync>;
// binds the state and interceptors to a handler, the handler type is still known so there is no extra dispatch
type Bind<S> = Box<dyn FnOnce(&Arc<S>, &[Interceptor<S>]) -> Handler>;

/// Rest of the chain passed to an interceptor, the following interceptors then the handler.
pub struct Next {
    handler: SharedHandler,
}

impl Next {
    pub fn run(self, request: Request) -> impl Future<Output = Result<Response, Exception>> + Send {
        (self.handler)(request)
    }
}

pub(crate) struct Route {
    pub(crate) name: &'static str,
    pub(crate) handler: Handler,
}

pub(crate) enum Matched<'a> {
    Found(&'static str, &'a Route),
    NotFound,
    MethodNotAllowed,
}

#[derive(Default)]
pub(crate) struct Routes {
    exact: HashMap<&'static str, Vec<(Method, Route)>>,
    // longest prefix first
    prefixes: Vec<(&'static str, Vec<(Method, Route)>)>,
}

impl Routes {
    pub(crate) fn find(&self, method: &Method, path: &str) -> Matched<'_> {
        let matched = self
            .exact
            .get_key_value(path)
            .map(|(path, routes)| (*path, routes))
            .or_else(|| self.prefixes.iter().find(|(prefix, _)| path.starts_with(prefix)).map(|(p, r)| (*p, r)));
        let Some((matched_path, routes)) = matched else {
            return Matched::NotFound;
        };
        let route = routes.iter().find(|(m, _)| m == method).or_else(|| {
            // HEAD falls back to GET, the server drops the body and keeps content-length
            (method == Method::HEAD).then(|| routes.iter().find(|(m, _)| m == Method::GET)).flatten()
        });
        match route {
            Some((_, route)) => Matched::Found(matched_path, route),
            None => Matched::MethodNotAllowed,
        }
    }

    fn add(&mut self, method: Method, path: &'static str, route: Route) {
        assert!(path.starts_with('/'), "path must start with '/', path={path}");
        let routes = self.exact.entry(path).or_default();
        assert!(!routes.iter().any(|(m, _)| *m == method), "duplicate route, method={method}, path={path}");
        routes.push((method, route));
    }

    fn add_prefix(&mut self, method: Method, prefix: &'static str, route: Route) {
        assert!(
            prefix.starts_with('/') && prefix.ends_with('/'),
            "prefix must start and end with '/', prefix={prefix}"
        );
        if let Some((_, routes)) = self.prefixes.iter_mut().find(|(p, _)| *p == prefix) {
            assert!(!routes.iter().any(|(m, _)| *m == method), "duplicate route, method={method}, prefix={prefix}");
            routes.push((method, route));
        } else {
            self.prefixes.push((prefix, vec![(method, route)]));
            self.prefixes.sort_by_key(|(p, _)| Reverse(p.len()));
        }
    }
}

/// Static path router, state is bound to handlers by `state`.
#[derive(Default)]
pub struct Router {
    pub(crate) routes: Routes,
}

impl Router {
    pub fn new() -> Router {
        Router::default()
    }

    /// Handlers registered on the returned router take `state` as first argument,
    /// one state per router, `merge` it with other routers.
    #[must_use]
    pub fn state<S>(state: Arc<S>) -> StateRouter<S>
    where
        S: Send + Sync + 'static,
    {
        StateRouter { state, routes: Vec::new(), interceptors: Vec::new() }
    }

    /// Serves files under `root` for GET / HEAD requests whose path starts with `prefix`,
    /// `prefix` must start and end with `/`, a path ending with `/` serves `index.html`.
    #[must_use]
    pub fn dir(mut self, prefix: &'static str, root: impl Into<PathBuf>) -> Router {
        let root: Arc<Path> = Arc::from(root.into());
        let handler: Handler = Box::new(move |request| Box::pin(file::serve_dir(prefix, Arc::clone(&root), request)));
        self.routes.add_prefix(Method::GET, prefix, Route { name: "framework::web::file::serve_dir", handler });
        self
    }

    /// Serves one file for GET / HEAD requests on `path`.
    #[must_use]
    pub fn file(mut self, path: &'static str, file: impl Into<PathBuf>) -> Router {
        let file: Arc<Path> = Arc::from(file.into());
        let handler: Handler = Box::new(move |request| Box::pin(file::serve_file(Arc::clone(&file), request)));
        self.routes.add(Method::GET, path, Route { name: "framework::web::file::serve_file", handler });
        self
    }

    /// Takes routes of another router, panics on duplicate routes.
    #[must_use]
    pub fn merge(mut self, other: impl Into<Router>) -> Router {
        let other = other.into();
        for (path, routes) in other.routes.exact {
            for (method, route) in routes {
                self.routes.add(method, path, route);
            }
        }
        for (prefix, routes) in other.routes.prefixes {
            for (method, route) in routes {
                self.routes.add_prefix(method, prefix, route);
            }
        }
        self
    }
}

/// Registers handlers sharing one state, see `Router::state`.
pub struct StateRouter<S> {
    state: Arc<S>,
    routes: Vec<StateRoute<S>>,
    interceptors: Vec<Interceptor<S>>,
}

struct StateRoute<S> {
    method: Method,
    path: &'static str,
    prefix: bool,
    name: &'static str,
    bind: Bind<S>,
}

/// Handlers are bound here, so interceptors apply to all routes whatever the registration order.
impl<S> From<StateRouter<S>> for Router {
    fn from(router: StateRouter<S>) -> Router {
        let mut routes = Routes::default();
        for StateRoute { method, path, prefix, name, bind } in router.routes {
            let route = Route { name, handler: bind(&router.state, &router.interceptors) };
            if prefix {
                routes.add_prefix(method, path, route);
            } else {
                routes.add(method, path, route);
            }
        }
        Router { routes }
    }
}

impl<S> StateRouter<S>
where
    S: Send + Sync + 'static,
{
    /// Wraps all handlers registered on this router, before or after this call,
    /// `async fn(state, request, next) -> Result<Response, Exception>`,
    /// call `next.run(request)` to proceed, interceptors run in registration order.
    #[must_use]
    pub fn intercept<F, Fut>(mut self, interceptor: F) -> StateRouter<S>
    where
        F: Fn(Arc<S>, Request, Next) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<Response, Exception>> + Send + 'static,
    {
        self.interceptors.push(Arc::new(move |state, request, next| Box::pin(interceptor(state, request, next))));
        self
    }

    #[must_use]
    pub fn route<F, Fut>(self, method: Method, path: &'static str, handler: F) -> StateRouter<S>
    where
        F: Fn(Arc<S>, Request) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<Response, Exception>> + Send + 'static,
    {
        let name = type_name_of_val(&handler);
        self.__route(method, path, name, handler)
    }

    // used by #[api], the handler is a closure, its type name is not meaningful as `fn` context
    #[doc(hidden)]
    #[must_use]
    pub fn __route<F, Fut>(
        mut self,
        method: Method,
        path: &'static str,
        name: &'static str,
        handler: F,
    ) -> StateRouter<S>
    where
        F: Fn(Arc<S>, Request) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<Response, Exception>> + Send + 'static,
    {
        let bind: Bind<S> = Box::new(move |state, interceptors| bind(state, interceptors, handler));
        self.routes.push(StateRoute { method, path, prefix: false, name, bind });
        self
    }

    /// Routes requests whose path starts with `prefix`, for paths with a variable tail, e.g. `/event/{app}`,
    /// `prefix` must start and end with `/`, the handler reads the tail from `request.path()`.
    #[must_use]
    pub fn prefix<F, Fut>(mut self, method: Method, prefix: &'static str, handler: F) -> StateRouter<S>
    where
        F: Fn(Arc<S>, Request) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<Response, Exception>> + Send + 'static,
    {
        let name = type_name_of_val(&handler);
        let bind: Bind<S> = Box::new(move |state, interceptors| bind(state, interceptors, handler));
        self.routes.push(StateRoute { method, path: prefix, prefix: true, name, bind });
        self
    }
}

fn bind<S, F, Fut>(state: &Arc<S>, interceptors: &[Interceptor<S>], handler: F) -> Handler
where
    S: Send + Sync + 'static,
    F: Fn(Arc<S>, Request) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<Response, Exception>> + Send + 'static,
{
    let handler_state = Arc::clone(state);
    if interceptors.is_empty() {
        return Box::new(move |request| Box::pin(handler(Arc::clone(&handler_state), request)));
    }
    // chain is built once, a request costs one boxed future per interceptor
    let mut next: SharedHandler = Arc::new(move |request| Box::pin(handler(Arc::clone(&handler_state), request)));
    for interceptor in interceptors.iter().rev() {
        let interceptor = Arc::clone(interceptor);
        let state = Arc::clone(state);
        let inner = next;
        next = Arc::new(move |request| interceptor(Arc::clone(&state), request, Next { handler: Arc::clone(&inner) }));
    }
    Box::new(move |request| next(request))
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn hello(_state: Arc<()>, _request: Request) -> Result<Response, Exception> {
        Ok(Response::text("hello"))
    }

    fn matched(routes: &Routes, method: &Method, path: &str) -> Option<&'static str> {
        match routes.find(method, path) {
            Matched::Found(path, _) => Some(path),
            Matched::NotFound | Matched::MethodNotAllowed => None,
        }
    }

    #[test]
    fn find() {
        let router: Router =
            Router::new().dir("/static/", "static").dir("/static/js/", "js").file("/favicon.ico", "favicon.ico").merge(
                Router::state(Arc::new(()))
                    .route(Method::GET, "/hello", hello)
                    .route(Method::POST, "/hello", hello)
                    .prefix(Method::POST, "/event/", hello),
            );
        let routes = &router.routes;

        assert_eq!(matched(routes, &Method::GET, "/hello"), Some("/hello"));
        assert_eq!(matched(routes, &Method::HEAD, "/hello"), Some("/hello"));
        assert_eq!(matched(routes, &Method::GET, "/static/js/app.js"), Some("/static/js/"));
        assert_eq!(matched(routes, &Method::GET, "/static/index.html"), Some("/static/"));
        assert_eq!(matched(routes, &Method::HEAD, "/favicon.ico"), Some("/favicon.ico"));
        assert!(matches!(routes.find(&Method::GET, "/unknown"), Matched::NotFound));
        assert!(matches!(routes.find(&Method::GET, "/static"), Matched::NotFound));

        assert!(matches!(routes.find(&Method::PUT, "/hello"), Matched::MethodNotAllowed));
        assert!(matches!(routes.find(&Method::POST, "/static/a.js"), Matched::MethodNotAllowed));

        assert_eq!(matched(routes, &Method::POST, "/event/app"), Some("/event/"));
        assert!(matches!(routes.find(&Method::GET, "/event/app"), Matched::MethodNotAllowed));
    }

    #[test]
    fn handler_name() {
        let router: Router = Router::state(Arc::new(())).route(Method::GET, "/hello", hello).into();
        let Matched::Found(_, route) = router.routes.find(&Method::GET, "/hello") else {
            panic!("expected route");
        };
        assert_eq!(route.name, "framework::web::router::tests::hello");
    }

    #[test]
    #[should_panic(expected = "duplicate route")]
    fn duplicate() {
        let _router: Router =
            Router::state(Arc::new(())).route(Method::GET, "/hello", hello).route(Method::GET, "/hello", hello).into();
    }

    #[test]
    #[should_panic(expected = "duplicate route")]
    fn merge_duplicate() {
        let _router = Router::new()
            .merge(Router::state(Arc::new(())).route(Method::GET, "/hello", hello))
            .merge(Router::state(Arc::new(())).route(Method::GET, "/hello", hello));
    }
}
