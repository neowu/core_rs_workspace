use proc_macro2::TokenStream;
use quote::format_ident;
use quote::quote;
use syn::Error;
use syn::FnArg;
use syn::Ident;
use syn::ItemTrait;
use syn::LitStr;
use syn::Result;
use syn::ReturnType;
use syn::TraitItem;
use syn::TraitItemFn;
use syn::Type;
use syn::parse_quote;
use syn::parse2;
use syn::token::RArrow;

use crate::util;

pub(crate) fn build(tokens: TokenStream) -> Result<TokenStream> {
    let mut trait_def: ItemTrait = parse2(tokens)?;
    let trait_ident = trait_def.ident.clone();
    let trait_vis = trait_def.vis.clone();
    let mod_ident = format_ident!("{}", util::to_snake_case(&trait_ident.to_string()));

    let mut route_statements = vec![];
    let mut client_methods = vec![];

    for item in &mut trait_def.items {
        let TraitItem::Fn(method) = item else {
            continue;
        };
        let model = parse_method(method)?;
        method.attrs.retain(|attr| {
            let path = attr.path();
            !path.is_ident("get") && !path.is_ident("post") && !path.is_ident("put") && !path.is_ident("path")
        });
        method.sig.asyncness = None;
        let response_type = &model.response_type;
        let new_return: Type = parse_quote!(impl ::core::future::Future<Output = #response_type> + Send);
        method.sig.output = ReturnType::Type(RArrow::default(), Box::new(new_return));
        route_statements.push(build_route_statement(&model));
        client_methods.push(build_client_method(&model));
    }

    Ok(quote! {
        #trait_def

        #trait_vis mod #mod_ident {
            use std::sync::Arc;

            use axum::Router;
            use axum::routing::MethodFilter;
            use axum::routing::on;
            use framework::http::HttpClient;
            use framework::web::api::ApiClient;
            use framework::web::api::__into_response;
            use framework::web::body::Json;
            use framework::web::body::Query;

            use super::*;

            pub fn route<T, S>(service: Arc<T>) -> Router<S>
            where
                T: #trait_ident + Send + Sync + 'static,
                S: Clone + Send + Sync + 'static,
            {
                let router = Router::<S>::new();
                #(#route_statements)*
                router
            }

            pub fn client(http_client: HttpClient, api_url: &'static str) -> impl #trait_ident {
                struct Client {
                    client: ApiClient,
                }
                impl #trait_ident for Client {
                    #(#client_methods)*
                }
                Client { client: ApiClient::new(http_client, api_url) }
            }
        }
    })
}

struct MethodModel {
    method_ident: Ident,
    path: LitStr,
    request_type: Option<Type>,
    response_type: Type,

    filter: TokenStream,
    extractor: TokenStream,
    client_call: Ident,
}

fn parse_method(method: &TraitItemFn) -> Result<MethodModel> {
    let method_ident = method.sig.ident.clone();

    if method.sig.asyncness.is_none() {
        return Err(Error::new_spanned(method, "method must be `async fn`"));
    }

    let mut http_method = None;
    let mut path = None;

    for attr in &method.attrs {
        let attr_path = attr.path();
        if attr_path.is_ident("get") {
            http_method = Some((quote!(MethodFilter::GET), quote!(Query), format_ident!("get")));
        } else if attr_path.is_ident("post") {
            http_method = Some((quote!(MethodFilter::POST), quote!(Json), format_ident!("post")));
        } else if attr_path.is_ident("put") {
            http_method = Some((quote!(MethodFilter::PUT), quote!(Json), format_ident!("put")));
        } else if attr_path.is_ident("path") {
            path = Some(attr.parse_args::<LitStr>()?);
        }
    }

    let (filter, extractor, client_call) = http_method.ok_or_else(|| {
        Error::new_spanned(method, "missing HTTP method attribute, expected #[get], #[post] or #[put]")
    })?;
    let path = path.ok_or_else(|| Error::new_spanned(method, "missing #[path(\"...\")] attribute"))?;

    let mut inputs = method.sig.inputs.iter();
    let first = inputs.next().ok_or_else(|| Error::new_spanned(method, "method must take &self"))?;
    if !matches!(first, FnArg::Receiver(_)) {
        return Err(Error::new_spanned(method, "method must take &self as first argument"));
    }

    let request_type = if let Some(request_arg) = inputs.next() {
        let FnArg::Typed(pat_type) = request_arg else {
            return Err(Error::new_spanned(method, "request parameter must be typed"));
        };
        if inputs.next().is_some() {
            return Err(Error::new_spanned(method, "method must take at most one request parameter"));
        }
        Some((*pat_type.ty).clone())
    } else {
        None
    };

    let ReturnType::Type(_, return_type) = &method.sig.output else {
        return Err(Error::new_spanned(method, "method must return `Result<..., Exception>`"));
    };
    let response_type = (**return_type).clone();

    Ok(MethodModel { method_ident, path, request_type, response_type, filter, extractor, client_call })
}

fn build_route_statement(model: &MethodModel) -> TokenStream {
    let method_ident = &model.method_ident;
    let filter = &model.filter;
    let path = &model.path;

    let handler = if let Some(request_type) = &model.request_type {
        let extractor = &model.extractor;
        quote! {
            async move |#extractor(req): #extractor<#request_type>| {
                let result = svc.#method_ident(req).await;
                __into_response(result)
            }
        }
    } else {
        quote! {
            async move || {
                let result = svc.#method_ident().await;
                __into_response(result)
            }
        }
    };

    quote! {
        let svc = Arc::clone(&service);
        let router = router.route(
            #path,
            on(#filter, #handler),
        );
    }
}

fn build_client_method(model: &MethodModel) -> TokenStream {
    let method_ident = &model.method_ident;
    let response_type = &model.response_type;
    let client_call = &model.client_call;
    let path = &model.path;

    if let Some(request_type) = &model.request_type {
        quote! {
            async fn #method_ident(&self, request: #request_type) -> #response_type {
                self.client.#client_call(#path, request).await
            }
        }
    } else {
        quote! {
            async fn #method_ident(&self) -> #response_type {
                self.client.#client_call(#path, ()).await
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use quote::quote;

    use super::build;

    #[test]
    fn build_api() {
        let source = quote! {
            #[api]
            pub trait UserService {
                #[get]
                #[path("/user/search")]
                async fn search(&self, request: SearchUserRequest) -> Result<SearchUserResponse, Exception>;

                #[post]
                #[path("/user/create")]
                async fn create(&self, request: CreateUserRequest) -> Result<CreateUserResponse, Exception>;

                #[put]
                #[path("/user/update")]
                async fn update(&self, request: UpdateUserRequest) -> Result<UpdateUserResponse, Exception>;
            }
        };

        let output = build(source).unwrap();

        assert_eq!(
            output.to_string(),
            quote! {
                #[api]
                pub trait UserService {
                    fn search(&self, request: SearchUserRequest) -> impl ::core::future::Future<Output = Result<SearchUserResponse, Exception> > + Send;
                    fn create(&self, request: CreateUserRequest) -> impl ::core::future::Future<Output = Result<CreateUserResponse, Exception> > + Send;
                    fn update(&self, request: UpdateUserRequest) -> impl ::core::future::Future<Output = Result<UpdateUserResponse, Exception> > + Send;
                }

                pub mod user_service {
                    use std::sync::Arc;

                    use axum::Router;
                    use axum::routing::MethodFilter;
                    use axum::routing::on;
                    use framework::http::HttpClient;
                    use framework::web::api::ApiClient;
                    use framework::web::api::__into_response;
                    use framework::web::body::Json;
                    use framework::web::body::Query;

                    use super::*;

                    pub fn route<T, S>(service: Arc<T>) -> Router<S>
                    where
                        T: UserService + Send + Sync + 'static,
                        S: Clone + Send + Sync + 'static,
                    {
                        let router = Router::<S>::new();
                        let svc = Arc::clone(&service);
                        let router = router.route(
                            "/user/search",
                            on(MethodFilter::GET, async move |Query(req): Query<SearchUserRequest>| {
                                let result = svc.search(req).await;
                                __into_response(result)
                            }),
                        );
                        let svc = Arc::clone(&service);
                        let router = router.route(
                            "/user/create",
                            on(MethodFilter::POST, async move |Json(req): Json<CreateUserRequest>| {
                                let result = svc.create(req).await;
                                __into_response(result)
                            }),
                        );
                        let svc = Arc::clone(&service);
                        let router = router.route(
                            "/user/update",
                            on(MethodFilter::PUT, async move |Json(req): Json<UpdateUserRequest>| {
                                let result = svc.update(req).await;
                                __into_response(result)
                            }),
                        );
                        router
                    }

                    pub fn client(http_client: HttpClient, api_url: &'static str) -> impl UserService {
                        struct Client {
                            client: ApiClient,
                        }
                        impl UserService for Client {
                            async fn search(&self, request: SearchUserRequest) -> Result<SearchUserResponse, Exception> {
                                self.client.get("/user/search", request).await
                            }
                            async fn create(&self, request: CreateUserRequest) -> Result<CreateUserResponse, Exception> {
                                self.client.post("/user/create", request).await
                            }
                            async fn update(&self, request: UpdateUserRequest) -> Result<UpdateUserResponse, Exception> {
                                self.client.put("/user/update", request).await
                            }
                        }
                        Client { client: ApiClient::new(http_client, api_url) }
                    }
                }
            }
            .to_string()
        );
    }

    #[test]
    fn build_api_with_optional() {
        let source = quote! {
            #[api]
            pub trait UserService {
                #[get]
                #[path("/user/get_all")]
                async fn get_all(&self) -> Result<GetAllUserResponse, Exception>;

                #[post]
                #[path("/user/create")]
                async fn create(&self, request: CreateUserRequest) -> Result<(), Exception>;
            }
        };

        let output = build(source).unwrap();

        assert_eq!(
            output.to_string(),
            quote! {
                #[api]
                pub trait UserService {
                    fn get_all(&self) -> impl ::core::future::Future<Output = Result<GetAllUserResponse, Exception> > + Send;
                    fn create(&self, request: CreateUserRequest) -> impl ::core::future::Future<Output = Result<(), Exception> > + Send;
                }

                pub mod user_service {
                    use std::sync::Arc;

                    use axum::Router;
                    use axum::routing::MethodFilter;
                    use axum::routing::on;
                    use framework::http::HttpClient;
                    use framework::web::api::ApiClient;
                    use framework::web::api::__into_response;
                    use framework::web::body::Json;
                    use framework::web::body::Query;

                    use super::*;

                    pub fn route<T, S>(service: Arc<T>) -> Router<S>
                    where
                        T: UserService + Send + Sync + 'static,
                        S: Clone + Send + Sync + 'static,
                    {
                        let router = Router::<S>::new();
                        let svc = Arc::clone(&service);
                        let router = router.route(
                            "/user/get_all",
                            on(MethodFilter::GET, async move || {
                                let result = svc.get_all().await;
                                __into_response(result)
                            }),
                        );
                        let svc = Arc::clone(&service);
                        let router = router.route(
                            "/user/create",
                            on(MethodFilter::POST, async move |Json(req): Json<CreateUserRequest>| {
                                let result = svc.create(req).await;
                                __into_response(result)
                            }),
                        );
                        router
                    }

                    pub fn client(http_client: HttpClient, api_url: &'static str) -> impl UserService {
                        struct Client {
                            client: ApiClient,
                        }
                        impl UserService for Client {
                            async fn get_all(&self) -> Result<GetAllUserResponse, Exception> {
                                self.client.get("/user/get_all", ()).await
                            }
                            async fn create(&self, request: CreateUserRequest) -> Result<(), Exception> {
                                self.client.post("/user/create", request).await
                            }
                        }
                        Client { client: ApiClient::new(http_client, api_url) }
                    }
                }
            }
            .to_string()
        );
    }
}
