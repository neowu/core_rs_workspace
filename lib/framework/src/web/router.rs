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

/// Static path router, state is bound per group of handlers by `state`.
#[derive(Default)]
pub struct Router {
    pub(crate) routes: Routes,
}

impl Router {
    pub fn new() -> Self {
        Self::default()
    }

    /// Handlers registered in `f` take `state` as first argument.
    #[must_use]
    pub fn state<S>(self, state: Arc<S>, f: impl FnOnce(StateRouter<S>) -> StateRouter<S>) -> Self
    where
        S: Send + Sync + 'static,
    {
        let StateRouter { routes, .. } = f(StateRouter { state, routes: self.routes });
        Self { routes }
    }

    /// Serves files under `root` for GET / HEAD requests whose path starts with `prefix`,
    /// `prefix` must start and end with `/`, a path ending with `/` serves `index.html`.
    #[must_use]
    pub fn dir(mut self, prefix: &'static str, root: impl Into<PathBuf>) -> Self {
        let root: Arc<Path> = Arc::from(root.into());
        let handler: Handler = Box::new(move |request| Box::pin(file::serve_dir(prefix, Arc::clone(&root), request)));
        self.routes.add_prefix(Method::GET, prefix, Route { name: "framework::web::file::serve_dir", handler });
        self
    }

    /// Serves one file for GET / HEAD requests on `path`.
    #[must_use]
    pub fn file(mut self, path: &'static str, file: impl Into<PathBuf>) -> Self {
        let file: Arc<Path> = Arc::from(file.into());
        let handler: Handler = Box::new(move |request| Box::pin(file::serve_file(Arc::clone(&file), request)));
        self.routes.add(Method::GET, path, Route { name: "framework::web::file::serve_file", handler });
        self
    }

    /// Takes routes of another router, panics on duplicate routes.
    #[must_use]
    pub fn merge(mut self, other: Router) -> Self {
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
    routes: Routes,
}

impl<S> StateRouter<S>
where
    S: Send + Sync + 'static,
{
    #[must_use]
    pub fn route<F, Fut>(self, method: Method, path: &'static str, handler: F) -> Self
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
    pub fn __route<F, Fut>(mut self, method: Method, path: &'static str, name: &'static str, handler: F) -> Self
    where
        F: Fn(Arc<S>, Request) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<Response, Exception>> + Send + 'static,
    {
        let handler = self.handler(handler);
        self.routes.add(method, path, Route { name, handler });
        self
    }

    /// Routes requests whose path starts with `prefix`, for paths with a variable tail, e.g. `/event/{app}`,
    /// `prefix` must start and end with `/`, the handler reads the tail from `request.path()`.
    #[must_use]
    pub fn prefix<F, Fut>(mut self, method: Method, prefix: &'static str, handler: F) -> Self
    where
        F: Fn(Arc<S>, Request) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<Response, Exception>> + Send + 'static,
    {
        let name = type_name_of_val(&handler);
        let handler = self.handler(handler);
        self.routes.add_prefix(method, prefix, Route { name, handler });
        self
    }

    fn handler<F, Fut>(&self, handler: F) -> Handler
    where
        F: Fn(Arc<S>, Request) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<Response, Exception>> + Send + 'static,
    {
        let state = Arc::clone(&self.state);
        Box::new(move |request| Box::pin(handler(Arc::clone(&state), request)))
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
        let router = Router::new()
            .state(Arc::new(()), |r| {
                r.get("/hello", hello).post("/hello", hello).prefix(Method::POST, "/event/", hello)
            })
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

        assert!(matches!(routes.find(&Method::PUT, "/hello"), Matched::MethodNotAllowed));
        assert!(matches!(routes.find(&Method::POST, "/static/a.js"), Matched::MethodNotAllowed));

        assert_eq!(matched(routes, &Method::POST, "/event/app"), Some("/event/"));
        assert!(matches!(routes.find(&Method::GET, "/event/app"), Matched::MethodNotAllowed));
    }

    #[test]
    fn handler_name() {
        let router = Router::new().state(Arc::new(()), |r| r.get("/hello", hello));
        let Matched::Found(_, route) = router.routes.find(&Method::GET, "/hello") else {
            panic!("expected route");
        };
        assert_eq!(route.name, "framework::web::router::tests::hello");
    }

    #[test]
    #[should_panic(expected = "duplicate route")]
    fn merge_duplicate() {
        let _router = Router::new()
            .state(Arc::new(()), |r| r.get("/hello", hello))
            .merge(Router::new().state(Arc::new(()), |r| r.get("/hello", hello)));
    }
}
