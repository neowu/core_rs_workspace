use std::any::type_name_of_val;
use std::cmp::Reverse;
use std::collections::HashMap;
use std::path::Path;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;

use framework::exception::Exception;
use http::Method;

use crate::file;
use crate::request::Request;
use crate::response::Response;

pub(crate) type HandlerFuture = Pin<Box<dyn Future<Output = Result<Response, Exception>> + Send>>;
type Handler = Box<dyn Fn(Request) -> HandlerFuture + Send + Sync>;

pub(crate) struct Route {
    pub(crate) name: &'static str,
    pub(crate) handler: Handler,
}

pub(crate) enum Matched<'a> {
    Found(&'static str, &'a Route),
    NotFound,
    MethodNotAllowed(Vec<&'a Method>),
}

#[derive(Default)]
pub(crate) struct Routes {
    exact: HashMap<&'static str, Vec<(Method, Route)>>,
    // longest prefix first, GET / HEAD only
    prefixes: Vec<(&'static str, Route)>,
}

impl Routes {
    pub(crate) fn find(&self, method: &Method, path: &str) -> Matched<'_> {
        if let Some((path, routes)) = self.exact.get_key_value(path) {
            let route = routes.iter().find(|(m, _)| m == method).or_else(|| {
                // same as axum, HEAD falls back to GET, the server drops the body and keeps content-length
                (method == Method::HEAD).then(|| routes.iter().find(|(m, _)| m == Method::GET)).flatten()
            });
            return match route {
                Some((_, route)) => Matched::Found(path, route),
                None => Matched::MethodNotAllowed(routes.iter().map(|(m, _)| m).collect()),
            };
        }
        for (prefix, route) in &self.prefixes {
            if path.starts_with(prefix) {
                return if method == Method::GET || method == Method::HEAD {
                    Matched::Found(prefix, route)
                } else {
                    Matched::MethodNotAllowed(vec![&Method::GET, &Method::HEAD])
                };
            }
        }
        Matched::NotFound
    }

    fn add(&mut self, method: Method, path: &'static str, route: Route) {
        assert!(path.starts_with('/'), "path must start with '/', path={path}");
        let routes = self.exact.entry(path).or_default();
        assert!(!routes.iter().any(|(m, _)| *m == method), "duplicate route, method={method}, path={path}");
        routes.push((method, route));
    }

    fn add_prefix(&mut self, prefix: &'static str, route: Route) {
        assert!(
            prefix.starts_with('/') && prefix.ends_with('/'),
            "prefix must start and end with '/', prefix={prefix}"
        );
        assert!(!self.prefixes.iter().any(|(p, _)| *p == prefix), "duplicate prefix, prefix={prefix}");
        self.prefixes.push((prefix, route));
        self.prefixes.sort_by_key(|(p, _)| Reverse(p.len()));
    }
}

/// Static path router, handlers share one state.
pub struct Router<S> {
    state: Arc<S>,
    pub(crate) routes: Routes,
}

impl<S> Router<S>
where
    S: Send + Sync + 'static,
{
    pub fn new(state: Arc<S>) -> Self {
        Self { state, routes: Routes::default() }
    }

    #[must_use]
    pub fn route<F, Fut>(mut self, method: Method, path: &'static str, handler: F) -> Self
    where
        F: Fn(Arc<S>, Request) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<Response, Exception>> + Send + 'static,
    {
        let name = type_name_of_val(&handler);
        let state = Arc::clone(&self.state);
        let handler: Handler = Box::new(move |request| Box::pin(handler(Arc::clone(&state), request)));
        self.routes.add(method, path, Route { name, handler });
        self
    }

    #[must_use]
    pub fn get<F, Fut>(self, path: &'static str, handler: F) -> Self
    where
        F: Fn(Arc<S>, Request) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<Response, Exception>> + Send + 'static,
    {
        self.route(Method::GET, path, handler)
    }

    #[must_use]
    pub fn post<F, Fut>(self, path: &'static str, handler: F) -> Self
    where
        F: Fn(Arc<S>, Request) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<Response, Exception>> + Send + 'static,
    {
        self.route(Method::POST, path, handler)
    }

    #[must_use]
    pub fn put<F, Fut>(self, path: &'static str, handler: F) -> Self
    where
        F: Fn(Arc<S>, Request) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<Response, Exception>> + Send + 'static,
    {
        self.route(Method::PUT, path, handler)
    }

    #[must_use]
    pub fn delete<F, Fut>(self, path: &'static str, handler: F) -> Self
    where
        F: Fn(Arc<S>, Request) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<Response, Exception>> + Send + 'static,
    {
        self.route(Method::DELETE, path, handler)
    }

    /// Serves files under `root` for GET / HEAD requests whose path starts with `prefix`,
    /// `prefix` must start and end with `/`, a path ending with `/` serves `index.html`.
    #[must_use]
    pub fn dir(mut self, prefix: &'static str, root: impl Into<PathBuf>) -> Self {
        let root: Arc<Path> = Arc::from(root.into());
        let handler: Handler = Box::new(move |request| Box::pin(file::serve_dir(prefix, Arc::clone(&root), request)));
        self.routes.add_prefix(prefix, Route { name: "framework_http::file::serve_dir", handler });
        self
    }

    /// Serves one file for GET / HEAD requests on `path`.
    #[must_use]
    pub fn file(mut self, path: &'static str, file: impl Into<PathBuf>) -> Self {
        let file: Arc<Path> = Arc::from(file.into());
        let handler: Handler = Box::new(move |request| Box::pin(file::serve_file(Arc::clone(&file), request)));
        self.routes.add(Method::GET, path, Route { name: "framework_http::file::serve_file", handler });
        self
    }

    /// Takes routes of a router with different state, panics on duplicate routes.
    #[must_use]
    pub fn merge<T>(mut self, other: Router<T>) -> Self {
        for (path, routes) in other.routes.exact {
            for (method, route) in routes {
                self.routes.add(method, path, route);
            }
        }
        for (prefix, route) in other.routes.prefixes {
            self.routes.add_prefix(prefix, route);
        }
        self
    }
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
            Matched::NotFound | Matched::MethodNotAllowed(_) => None,
        }
    }

    #[test]
    fn find() {
        let router = Router::new(Arc::new(()))
            .get("/hello", hello)
            .post("/hello", hello)
            .dir("/static/", "static")
            .dir("/static/js/", "js")
            .file("/favicon.ico", "favicon.ico");
        let routes = &router.routes;

        assert_eq!(matched(routes, &Method::GET, "/hello"), Some("/hello"));
        assert_eq!(matched(routes, &Method::HEAD, "/hello"), Some("/hello"));
        assert_eq!(matched(routes, &Method::GET, "/static/js/app.js"), Some("/static/js/"));
        assert_eq!(matched(routes, &Method::GET, "/static/index.html"), Some("/static/"));
        assert_eq!(matched(routes, &Method::HEAD, "/favicon.ico"), Some("/favicon.ico"));
        assert!(matches!(routes.find(&Method::GET, "/unknown"), Matched::NotFound));
        assert!(matches!(routes.find(&Method::GET, "/static"), Matched::NotFound));

        let Matched::MethodNotAllowed(allowed) = routes.find(&Method::PUT, "/hello") else {
            panic!("expected method not allowed");
        };
        assert_eq!(allowed, vec![&Method::GET, &Method::POST]);
        assert!(matches!(routes.find(&Method::POST, "/static/a.js"), Matched::MethodNotAllowed(_)));
    }

    #[test]
    fn handler_name() {
        let router = Router::new(Arc::new(())).get("/hello", hello);
        let Matched::Found(_, route) = router.routes.find(&Method::GET, "/hello") else {
            panic!("expected route");
        };
        assert_eq!(route.name, "framework_http::router::tests::hello");
    }

    #[test]
    #[should_panic(expected = "duplicate route")]
    fn merge_duplicate() {
        let _router: Router<()> =
            Router::new(Arc::new(())).get("/hello", hello).merge(Router::new(Arc::new(())).get("/hello", hello));
    }
}
