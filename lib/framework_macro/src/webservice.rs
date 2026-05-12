use proc_macro2::TokenStream;
use quote::format_ident;
use quote::quote;
use syn::AngleBracketedGenericArguments;
use syn::AssocType;
use syn::Error;
use syn::FnArg;
use syn::GenericArgument;
use syn::Ident;
use syn::ItemTrait;
use syn::LitStr;
use syn::PathArguments;
use syn::Result;
use syn::ReturnType;
use syn::TraitItem;
use syn::TraitItemFn;
use syn::Type;
use syn::TypeParamBound;
use syn::parse_quote;
use syn::parse2;
use syn::token::RArrow;

use crate::util;

pub(crate) fn build(tokens: TokenStream) -> Result<TokenStream> {
    let mut trait_def: ItemTrait = parse2(tokens)?;
    let trait_ident = trait_def.ident.clone();
    let trait_vis = trait_def.vis.clone();
    let mod_ident = format_ident!("{}", util::to_snake_case(&trait_ident.to_string()));

    let mut route_stmts = vec![];
    let mut client_methods = vec![];

    for item in &mut trait_def.items {
        let TraitItem::Fn(method) = item else {
            continue;
        };
        let info = parse_method(method)?;
        method.attrs.retain(|attr| {
            let path = attr.path();
            !path.is_ident("get") && !path.is_ident("post") && !path.is_ident("put") && !path.is_ident("path")
        });
        if method.sig.asyncness.take().is_some() {
            let response_type = &info.response_type;
            let new_return: Type = parse_quote!(impl ::core::future::Future<Output = #response_type> + Send);
            method.sig.output = ReturnType::Type(RArrow::default(), Box::new(new_return));
        }
        route_stmts.push(build_route_stmt(&info));
        client_methods.push(build_client_method(&info));
    }

    Ok(quote! {
        #trait_def

        #[allow(clippy::wildcard_imports, clippy::needless_pass_by_value)]
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

            pub fn route<T>(service: Arc<T>) -> Router
            where
                T: #trait_ident + Send + Sync + 'static,
            {
                let router = Router::new();
                #(#route_stmts)*
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

struct MethodInfo {
    method_ident: Ident,
    http_method: HttpMethod,
    path: LitStr,
    request_type: Type,
    response_type: Type,
}

enum HttpMethod {
    Get,
    Post,
    Put,
}

impl HttpMethod {
    fn filter(&self) -> TokenStream {
        match self {
            HttpMethod::Get => quote!(MethodFilter::GET),
            HttpMethod::Post => quote!(MethodFilter::POST),
            HttpMethod::Put => quote!(MethodFilter::PUT),
        }
    }

    fn extractor(&self) -> TokenStream {
        match self {
            HttpMethod::Get => quote!(Query),
            HttpMethod::Post | HttpMethod::Put => quote!(Json),
        }
    }

    fn client_call(&self) -> Ident {
        match self {
            HttpMethod::Get => format_ident!("get"),
            HttpMethod::Post => format_ident!("post"),
            HttpMethod::Put => format_ident!("put"),
        }
    }
}

fn parse_method(method: &TraitItemFn) -> Result<MethodInfo> {
    let method_ident = method.sig.ident.clone();

    let mut http_method = None;
    let mut path = None;

    for attr in &method.attrs {
        let attr_path = attr.path();
        if attr_path.is_ident("get") {
            http_method = Some(HttpMethod::Get);
        } else if attr_path.is_ident("post") {
            http_method = Some(HttpMethod::Post);
        } else if attr_path.is_ident("put") {
            http_method = Some(HttpMethod::Put);
        } else if attr_path.is_ident("path") {
            path = Some(attr.parse_args::<LitStr>()?);
        }
    }

    let http_method = http_method.ok_or_else(|| {
        Error::new_spanned(method, "missing HTTP method attribute, expected #[get], #[post] or #[put]")
    })?;
    let path = path.ok_or_else(|| Error::new_spanned(method, "missing #[path(\"...\")] attribute"))?;

    let mut inputs = method.sig.inputs.iter();
    let first = inputs.next().ok_or_else(|| Error::new_spanned(method, "method must take &self"))?;
    if !matches!(first, FnArg::Receiver(_)) {
        return Err(Error::new_spanned(method, "method must take &self as first argument"));
    }

    let request_arg =
        inputs.next().ok_or_else(|| Error::new_spanned(method, "method must take exactly one request parameter"))?;
    let FnArg::Typed(pat_type) = request_arg else {
        return Err(Error::new_spanned(method, "request parameter must be typed"));
    };
    if inputs.next().is_some() {
        return Err(Error::new_spanned(method, "method must take exactly one request parameter"));
    }
    let request_type = (*pat_type.ty).clone();

    let ReturnType::Type(_, return_type) = &method.sig.output else {
        return Err(Error::new_spanned(method, "method must return `Result<..., Exception>`"));
    };
    let response_type =
        if method.sig.asyncness.is_some() { (**return_type).clone() } else { extract_future_output(return_type)? };

    Ok(MethodInfo { method_ident, http_method, path, request_type, response_type })
}

fn extract_future_output(future_type: &Type) -> Result<Type> {
    let Type::ImplTrait(impl_trait) = future_type else {
        return Err(Error::new_spanned(
            future_type,
            "method return type must be `impl Future<Output = Result<..., Exception>> + Send`",
        ));
    };
    for bound in &impl_trait.bounds {
        let TypeParamBound::Trait(trait_bound) = bound else {
            continue;
        };
        let Some(last) = trait_bound.path.segments.last() else {
            continue;
        };
        if last.ident != "Future" {
            continue;
        }
        let PathArguments::AngleBracketed(AngleBracketedGenericArguments { args, .. }) = &last.arguments else {
            continue;
        };
        for arg in args {
            if let GenericArgument::AssocType(AssocType { ident, ty, .. }) = arg
                && ident == "Output"
            {
                return Ok(ty.clone());
            }
        }
    }
    Err(Error::new_spanned(
        future_type,
        "method return type must be `impl Future<Output = Result<..., Exception>> + Send`",
    ))
}

fn build_route_stmt(info: &MethodInfo) -> TokenStream {
    let method_ident = &info.method_ident;
    let filter = info.http_method.filter();
    let extractor = info.http_method.extractor();
    let path = &info.path;
    let request_type = &info.request_type;

    quote! {
        let svc = Arc::clone(&service);
        let router = router.route(
            #path,
            on(#filter, async move |#extractor(req): #extractor<#request_type>| {
                let result = svc.#method_ident(req).await;
                __into_response(result)
            }),
        );
    }
}

fn build_client_method(info: &MethodInfo) -> TokenStream {
    let method_ident = &info.method_ident;
    let request_type = &info.request_type;
    let response_type = &info.response_type;
    let client_call = info.http_method.client_call();
    let path = &info.path;

    quote! {
        async fn #method_ident(&self, request: #request_type) -> #response_type {
            self.client.#client_call(#path, request).await
        }
    }
}

#[cfg(test)]
mod tests {
    use quote::quote;

    use super::build;

    #[test]
    fn build_webservice() {
        let source = quote! {
            #[webservice]
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
                #[webservice]
                pub trait UserService {
                    fn search(&self, request: SearchUserRequest) -> impl ::core::future::Future<Output = Result<SearchUserResponse, Exception> > + Send;
                    fn create(&self, request: CreateUserRequest) -> impl ::core::future::Future<Output = Result<CreateUserResponse, Exception> > + Send;
                    fn update(&self, request: UpdateUserRequest) -> impl ::core::future::Future<Output = Result<UpdateUserResponse, Exception> > + Send;
                }

                #[allow(clippy::wildcard_imports, clippy::needless_pass_by_value)]
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

                    pub fn route<T>(service: Arc<T>) -> Router
                    where
                        T: UserService + Send + Sync + 'static,
                    {
                        let router = Router::new();
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
}
