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

pub(crate) fn build(tokens: TokenStream) -> Result<TokenStream> {
    let mut trait_def: ItemTrait = parse2(tokens)?;
    let trait_ident = trait_def.ident.clone();
    let trait_vis = trait_def.vis.clone();
    let client_ident = format_ident!("{trait_ident}Client");

    let mut route_statements = vec![];
    let mut client_methods = vec![];
    let mut operation_definitions = vec![];

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
        operation_definitions.push(build_operation_definition(&model));
    }

    trait_def.items.push(TraitItem::Fn(parse_quote! {
        fn route(service: ::std::sync::Arc<Self>) -> ::axum::Router
        where
            Self: Sized + Send + Sync + 'static,
        {
            use std::sync::Arc;

            use axum::Router;
            use axum::routing::MethodFilter;
            use axum::routing::on;
            use framework::context;
            use framework::web::api::__into_response;
            use framework::web::body::Json;
            use framework::web::body::Query;

            let router = Router::new();
            #(#route_statements)*
            router
        }
    }));

    let trait_name = trait_ident.to_string();
    trait_def.items.push(TraitItem::Fn(parse_quote! {
        fn api_definition(registry: &mut ::framework::api::TypeRegistry) -> ::framework::api::ServiceDefinition
        where
            Self: Sized,
        {
            ::framework::api::ServiceDefinition {
                name: #trait_name.to_owned(),
                operations: vec![#(#operation_definitions),*],
            }
        }
    }));

    Ok(quote! {
        #trait_def

        #trait_vis struct #client_ident {
            client: ::framework::web::api::ApiClient,
        }

        impl #client_ident {
            #trait_vis fn new(http_client: ::framework::http::HttpClient, api_url: String) -> Self {
                Self { client: ::framework::web::api::ApiClient::new(http_client, api_url) }
            }
        }

        impl #trait_ident for #client_ident {
            #(#client_methods)*
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
    http_method: &'static str,
}

fn parse_method(method: &TraitItemFn) -> Result<MethodModel> {
    let method_ident = method.sig.ident.clone();

    if method.sig.asyncness.is_none() {
        return Err(Error::new_spanned(method, "method must be `async fn`"));
    }

    if method_ident == "route" || method_ident == "api_definition" {
        return Err(Error::new_spanned(method, format!("method name `{method_ident}` is reserved by #[api]")));
    }

    let mut http_method = None;
    let mut path = None;

    for attr in &method.attrs {
        let attr_path = attr.path();
        if attr_path.is_ident("get") {
            http_method = Some((quote!(MethodFilter::GET), quote!(Query), format_ident!("get"), "GET"));
        } else if attr_path.is_ident("post") {
            http_method = Some((quote!(MethodFilter::POST), quote!(Json), format_ident!("post"), "POST"));
        } else if attr_path.is_ident("put") {
            http_method = Some((quote!(MethodFilter::PUT), quote!(Json), format_ident!("put"), "PUT"));
        } else if attr_path.is_ident("path") {
            path = Some(attr.parse_args::<LitStr>()?);
        }
    }

    let (filter, extractor, client_call, http_method) = http_method.ok_or_else(|| {
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

    Ok(MethodModel { method_ident, path, request_type, response_type, filter, extractor, client_call, http_method })
}

fn build_operation_definition(model: &MethodModel) -> TokenStream {
    let name = model.method_ident.to_string();
    let http_method = model.http_method;
    let path = &model.path;
    let response_type = &model.response_type;

    let request = if let Some(request_type) = &model.request_type {
        quote!(Some(<#request_type as ::framework::api::ApiType>::type_ref(registry)))
    } else {
        quote!(None)
    };

    quote! {
        ::framework::api::OperationDefinition {
            name: #name.to_owned(),
            method: #http_method.to_owned(),
            path: #path.to_owned(),
            request: #request,
            response: <#response_type as ::framework::api::ApiType>::optional_type_ref(registry),
        }
    }
}

fn build_route_statement(model: &MethodModel) -> TokenStream {
    let method_ident = &model.method_ident;
    let filter = &model.filter;
    let path = &model.path;
    let fn_format = format!("{{}}::{method_ident}");

    let handler = if let Some(request_type) = &model.request_type {
        let extractor = &model.extractor;
        quote! {
            async move |#extractor(req): #extractor<#request_type>| {
                context!(fn = format!(#fn_format, std::any::type_name::<Self>()));
                let result = svc.#method_ident(req).await;
                __into_response(result)
            }
        }
    } else {
        quote! {
            async move || {
                context!(fn = format!(#fn_format, std::any::type_name::<Self>()));
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
    #[allow(clippy::too_many_lines)]
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

                    fn route(service: ::std::sync::Arc<Self>) -> ::axum::Router
                    where
                        Self: Sized + Send + Sync + 'static,
                    {
                        use std::sync::Arc;

                        use axum::Router;
                        use axum::routing::MethodFilter;
                        use axum::routing::on;
                        use framework::context;
                        use framework::web::api::__into_response;
                        use framework::web::body::Json;
                        use framework::web::body::Query;

                        let router = Router::new();
                        let svc = Arc::clone(&service);
                        let router = router.route(
                            "/user/search",
                            on(MethodFilter::GET, async move |Query(req): Query<SearchUserRequest>| {
                                context!(fn = format!("{}::search", std::any::type_name::<Self>()));
                                let result = svc.search(req).await;
                                __into_response(result)
                            }),
                        );
                        let svc = Arc::clone(&service);
                        let router = router.route(
                            "/user/create",
                            on(MethodFilter::POST, async move |Json(req): Json<CreateUserRequest>| {
                                context!(fn = format!("{}::create", std::any::type_name::<Self>()));
                                let result = svc.create(req).await;
                                __into_response(result)
                            }),
                        );
                        let svc = Arc::clone(&service);
                        let router = router.route(
                            "/user/update",
                            on(MethodFilter::PUT, async move |Json(req): Json<UpdateUserRequest>| {
                                context!(fn = format!("{}::update", std::any::type_name::<Self>()));
                                let result = svc.update(req).await;
                                __into_response(result)
                            }),
                        );
                        router
                    }

                    fn api_definition(registry: &mut ::framework::api::TypeRegistry) -> ::framework::api::ServiceDefinition
                    where
                        Self: Sized,
                    {
                        ::framework::api::ServiceDefinition {
                            name: "UserService".to_owned(),
                            operations: vec![
                                ::framework::api::OperationDefinition {
                                    name: "search".to_owned(),
                                    method: "GET".to_owned(),
                                    path: "/user/search".to_owned(),
                                    request: Some(<SearchUserRequest as ::framework::api::ApiType>::type_ref(registry)),
                                    response: <Result<SearchUserResponse, Exception> as ::framework::api::ApiType>::optional_type_ref(registry),
                                },
                                ::framework::api::OperationDefinition {
                                    name: "create".to_owned(),
                                    method: "POST".to_owned(),
                                    path: "/user/create".to_owned(),
                                    request: Some(<CreateUserRequest as ::framework::api::ApiType>::type_ref(registry)),
                                    response: <Result<CreateUserResponse, Exception> as ::framework::api::ApiType>::optional_type_ref(registry),
                                },
                                ::framework::api::OperationDefinition {
                                    name: "update".to_owned(),
                                    method: "PUT".to_owned(),
                                    path: "/user/update".to_owned(),
                                    request: Some(<UpdateUserRequest as ::framework::api::ApiType>::type_ref(registry)),
                                    response: <Result<UpdateUserResponse, Exception> as ::framework::api::ApiType>::optional_type_ref(registry),
                                }
                            ],
                        }
                    }
                }

                pub struct UserServiceClient {
                    client: ::framework::web::api::ApiClient,
                }

                impl UserServiceClient {
                    pub fn new(http_client: ::framework::http::HttpClient, api_url: String) -> Self {
                        Self { client: ::framework::web::api::ApiClient::new(http_client, api_url) }
                    }
                }

                impl UserService for UserServiceClient {
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

                    fn route(service: ::std::sync::Arc<Self>) -> ::axum::Router
                    where
                        Self: Sized + Send + Sync + 'static,
                    {
                        use std::sync::Arc;

                        use axum::Router;
                        use axum::routing::MethodFilter;
                        use axum::routing::on;
                        use framework::context;
                        use framework::web::api::__into_response;
                        use framework::web::body::Json;
                        use framework::web::body::Query;

                        let router = Router::new();
                        let svc = Arc::clone(&service);
                        let router = router.route(
                            "/user/get_all",
                            on(MethodFilter::GET, async move | | {
                                context!(fn = format!("{}::get_all", std::any::type_name::<Self>()));
                                let result = svc.get_all().await;
                                __into_response(result)
                            }),
                        );
                        let svc = Arc::clone(&service);
                        let router = router.route(
                            "/user/create",
                            on(MethodFilter::POST, async move |Json(req): Json<CreateUserRequest>| {
                                context!(fn = format!("{}::create", std::any::type_name::<Self>()));
                                let result = svc.create(req).await;
                                __into_response(result)
                            }),
                        );
                        router
                    }

                    fn api_definition(registry: &mut ::framework::api::TypeRegistry) -> ::framework::api::ServiceDefinition
                    where
                        Self: Sized,
                    {
                        ::framework::api::ServiceDefinition {
                            name: "UserService".to_owned(),
                            operations: vec![
                                ::framework::api::OperationDefinition {
                                    name: "get_all".to_owned(),
                                    method: "GET".to_owned(),
                                    path: "/user/get_all".to_owned(),
                                    request: None,
                                    response: <Result<GetAllUserResponse, Exception> as ::framework::api::ApiType>::optional_type_ref(registry),
                                },
                                ::framework::api::OperationDefinition {
                                    name: "create".to_owned(),
                                    method: "POST".to_owned(),
                                    path: "/user/create".to_owned(),
                                    request: Some(<CreateUserRequest as ::framework::api::ApiType>::type_ref(registry)),
                                    response: <Result<(), Exception> as ::framework::api::ApiType>::optional_type_ref(registry),
                                }
                            ],
                        }
                    }
                }

                pub struct UserServiceClient {
                    client: ::framework::web::api::ApiClient,
                }

                impl UserServiceClient {
                    pub fn new(http_client: ::framework::http::HttpClient, api_url: String) -> Self {
                        Self { client: ::framework::web::api::ApiClient::new(http_client, api_url) }
                    }
                }

                impl UserService for UserServiceClient {
                    async fn get_all(&self) -> Result<GetAllUserResponse, Exception> {
                        self.client.get("/user/get_all", ()).await
                    }
                    async fn create(&self, request: CreateUserRequest) -> Result<(), Exception> {
                        self.client.post("/user/create", request).await
                    }
                }
            }
            .to_string()
        );
    }

    #[test]
    fn build_api_with_reserved_method_name() {
        let source = quote! {
            pub trait UserService {
                #[get]
                #[path("/user/route")]
                async fn route(&self) -> Result<(), Exception>;
            }
        };

        let error = build(source).unwrap_err();
        assert_eq!(error.to_string(), "method name `route` is reserved by #[api]");
    }

    #[test]
    fn build_api_with_reserved_api_definition_name() {
        let source = quote! {
            pub trait UserService {
                #[get]
                #[path("/user/api")]
                async fn api_definition(&self) -> Result<(), Exception>;
            }
        };

        let error = build(source).unwrap_err();
        assert_eq!(error.to_string(), "method name `api_definition` is reserved by #[api]");
    }
}
