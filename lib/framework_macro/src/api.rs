use proc_macro2::TokenStream;
use quote::format_ident;
use quote::quote;
use quote::quote_spanned;
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
use syn::spanned::Spanned as _;
use syn::token::RArrow;

pub(crate) fn build(tokens: TokenStream) -> Result<TokenStream> {
    let mut trait_def: ItemTrait = parse2(tokens)?;
    let trait_ident = trait_def.ident.clone();
    let trait_vis = trait_def.vis.clone();
    let client_ident = format_ident!("{trait_ident}Client");

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
        client_methods.push(build_client_method(&client_ident, &model));
    }

    trait_def.items.push(TraitItem::Fn(parse_quote! {
        fn route(service: ::std::sync::Arc<Self>) -> ::framework::web::router::Router
        where
            Self: Sized + Send + Sync + 'static,
        {
            use std::sync::Arc;

            use framework::http::Method;
            use framework::web::api::__into_response;
            use framework::web::request::Request;
            use framework::web::router::Router;

            Router::new().state(service, |router| {
                #(#route_statements)*
                router
            })
        }
    }));

    Ok(quote! {
        #trait_def

        #trait_vis struct #client_ident {
            client: ::framework::web::api::ApiClient,
        }

        impl #client_ident {
            #trait_vis fn new(http_client: ::framework::http::HttpClient, api_url: String) -> Self {
                Self { client: ::framework::web::api::ApiClient::__new(http_client, api_url) }
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

    http_method: TokenStream,
    // binding and parsing of the framework request, `mut` only to read the body
    request_binding: TokenStream,
    parse_request: TokenStream,
    client_call: Ident,
}

fn parse_method(method: &TraitItemFn) -> Result<MethodModel> {
    let method_ident = method.sig.ident.clone();

    if method.sig.asyncness.is_none() {
        return Err(Error::new_spanned(method, "method must be `async fn`"));
    }

    if method_ident == "route" {
        return Err(Error::new_spanned(method, "method name `route` is reserved by #[api]"));
    }

    let mut method_attr = None;
    let mut path = None;

    for attr in &method.attrs {
        let attr_path = attr.path();
        if attr_path.is_ident("get") {
            method_attr =
                Some((quote!(Method::GET), quote!(request), quote!(request.query()?), format_ident!("__get")));
        } else if attr_path.is_ident("post") {
            method_attr = Some((
                quote!(Method::POST),
                quote!(mut request),
                quote!(request.json().await?),
                format_ident!("__post"),
            ));
        } else if attr_path.is_ident("put") {
            method_attr =
                Some((quote!(Method::PUT), quote!(mut request), quote!(request.json().await?), format_ident!("__put")));
        } else if attr_path.is_ident("path") {
            path = Some(attr.parse_args::<LitStr>()?);
        }
    }

    let (http_method, request_binding, parse_request, client_call) = method_attr.ok_or_else(|| {
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

    Ok(MethodModel {
        method_ident,
        path,
        request_type,
        response_type,
        http_method,
        request_binding,
        parse_request,
        client_call,
    })
}

fn build_route_statement(model: &MethodModel) -> TokenStream {
    let method_ident = &model.method_ident;
    let http_method = &model.http_method;
    let path = &model.path;
    let fn_format = format!("{{}}::{method_ident}");

    let handler = if let Some(request_type) = &model.request_type {
        let request_binding = &model.request_binding;
        let parse_request = &model.parse_request;
        // spanned to request type, so missing Validator impl is reported on the trait method
        let validate = quote_spanned! {request_type.span()=> framework::validate::Validator::validate(&req)};
        quote! {
            |service: Arc<Self>, #request_binding: Request| async move {
                let req: #request_type = #parse_request;
                #validate?;
                __into_response(service.#method_ident(req).await)
            }
        }
    } else {
        quote! {
            |service: Arc<Self>, _request: Request| async move {
                __into_response(service.#method_ident().await)
            }
        }
    };

    quote! {
        let fn_name: &'static str = format!(#fn_format, std::any::type_name::<Self>()).leak();
        let router = router.__route(#http_method, #path, fn_name, #handler);
    }
}

fn build_client_method(client_ident: &Ident, model: &MethodModel) -> TokenStream {
    let method_ident = &model.method_ident;
    let response_type = &model.response_type;
    let client_call = &model.client_call;
    let path = &model.path;
    let fn_suffix = format!("::{client_ident}::{method_ident}");

    if let Some(request_type) = &model.request_type {
        quote! {
            async fn #method_ident(&self, request: #request_type) -> #response_type {
                ::framework::log!(concat!("call http api, fn=", module_path!(), #fn_suffix));
                self.client.#client_call(#path, request).await
            }
        }
    } else {
        quote! {
            async fn #method_ident(&self) -> #response_type {
                ::framework::log!(concat!("call http api, fn=", module_path!(), #fn_suffix));
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

                    fn route(service: ::std::sync::Arc<Self>) -> ::framework::web::router::Router
                    where
                        Self: Sized + Send + Sync + 'static,
                    {
                        use std::sync::Arc;

                        use framework::http::Method;
                        use framework::web::api::__into_response;
                        use framework::web::request::Request;
                        use framework::web::router::Router;

                        Router::new().state(service, |router| {
                        let fn_name: &'static str = format!("{}::search", std::any::type_name::<Self>()).leak();
                        let router = router.__route(Method::GET, "/user/search", fn_name, |service: Arc<Self>, request: Request| async move {
                            let req: SearchUserRequest = request.query()?;
                            framework::validate::Validator::validate(&req)?;
                            __into_response(service.search(req).await)
                        });
                        let fn_name: &'static str = format!("{}::create", std::any::type_name::<Self>()).leak();
                        let router = router.__route(Method::POST, "/user/create", fn_name, |service: Arc<Self>, mut request: Request| async move {
                            let req: CreateUserRequest = request.json().await?;
                            framework::validate::Validator::validate(&req)?;
                            __into_response(service.create(req).await)
                        });
                        let fn_name: &'static str = format!("{}::update", std::any::type_name::<Self>()).leak();
                        let router = router.__route(Method::PUT, "/user/update", fn_name, |service: Arc<Self>, mut request: Request| async move {
                            let req: UpdateUserRequest = request.json().await?;
                            framework::validate::Validator::validate(&req)?;
                            __into_response(service.update(req).await)
                        });
                        router
                        })
                    }
                }

                pub struct UserServiceClient {
                    client: ::framework::web::api::ApiClient,
                }

                impl UserServiceClient {
                    pub fn new(http_client: ::framework::http::HttpClient, api_url: String) -> Self {
                        Self { client: ::framework::web::api::ApiClient::__new(http_client, api_url) }
                    }
                }

                impl UserService for UserServiceClient {
                    async fn search(&self, request: SearchUserRequest) -> Result<SearchUserResponse, Exception> {
                        ::framework::log!(concat!("call http api, fn=", module_path!(), "::UserServiceClient::search"));
                        self.client.__get("/user/search", request).await
                    }
                    async fn create(&self, request: CreateUserRequest) -> Result<CreateUserResponse, Exception> {
                        ::framework::log!(concat!("call http api, fn=", module_path!(), "::UserServiceClient::create"));
                        self.client.__post("/user/create", request).await
                    }
                    async fn update(&self, request: UpdateUserRequest) -> Result<UpdateUserResponse, Exception> {
                        ::framework::log!(concat!("call http api, fn=", module_path!(), "::UserServiceClient::update"));
                        self.client.__put("/user/update", request).await
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

                    fn route(service: ::std::sync::Arc<Self>) -> ::framework::web::router::Router
                    where
                        Self: Sized + Send + Sync + 'static,
                    {
                        use std::sync::Arc;

                        use framework::http::Method;
                        use framework::web::api::__into_response;
                        use framework::web::request::Request;
                        use framework::web::router::Router;

                        Router::new().state(service, |router| {
                        let fn_name: &'static str = format!("{}::get_all", std::any::type_name::<Self>()).leak();
                        let router = router.__route(Method::GET, "/user/get_all", fn_name, |service: Arc<Self>, _request: Request| async move {
                            __into_response(service.get_all().await)
                        });
                        let fn_name: &'static str = format!("{}::create", std::any::type_name::<Self>()).leak();
                        let router = router.__route(Method::POST, "/user/create", fn_name, |service: Arc<Self>, mut request: Request| async move {
                            let req: CreateUserRequest = request.json().await?;
                            framework::validate::Validator::validate(&req)?;
                            __into_response(service.create(req).await)
                        });
                        router
                        })
                    }
                }

                pub struct UserServiceClient {
                    client: ::framework::web::api::ApiClient,
                }

                impl UserServiceClient {
                    pub fn new(http_client: ::framework::http::HttpClient, api_url: String) -> Self {
                        Self { client: ::framework::web::api::ApiClient::__new(http_client, api_url) }
                    }
                }

                impl UserService for UserServiceClient {
                    async fn get_all(&self) -> Result<GetAllUserResponse, Exception> {
                        ::framework::log!(concat!("call http api, fn=", module_path!(), "::UserServiceClient::get_all"));
                        self.client.__get("/user/get_all", ()).await
                    }
                    async fn create(&self, request: CreateUserRequest) -> Result<(), Exception> {
                        ::framework::log!(concat!("call http api, fn=", module_path!(), "::UserServiceClient::create"));
                        self.client.__post("/user/create", request).await
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
}
