use proc_macro2::TokenStream;
use quote::format_ident;
use quote::quote;
use syn::Attribute;
use syn::Error;
use syn::Expr;
use syn::ExprLit;
use syn::FnArg;
use syn::Ident;
use syn::ItemTrait;
use syn::Lit;
use syn::LitStr;
use syn::Meta;
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

    let mut handler_statements = vec![];
    let mut client_methods = vec![];

    for item in &mut trait_def.items {
        let TraitItem::Fn(method) = item else {
            continue;
        };
        let model = parse_method(method)?;
        method.attrs.retain(|attr| !attr.path().is_ident("subject"));
        method.sig.asyncness = None;
        let response_type = &model.response_type;
        let new_return: Type = parse_quote!(impl ::core::future::Future<Output = #response_type> + Send);
        method.sig.output = ReturnType::Type(RArrow::default(), Box::new(new_return));
        handler_statements.push(build_handler_statement(&model));
        client_methods.push(build_client_method(&model));
    }

    Ok(quote! {
        #trait_def

        #trait_vis mod #mod_ident {
            use std::sync::Arc;

            use framework::context;
            use framework_nats::async_nats;
            use framework_nats::service::Service;
            use framework_nats::service::ServiceClient;

            use super::*;

            pub fn service<T>(nats_client: async_nats::Client, service: Arc<T>) -> Service
            where
                T: #trait_ident + Send + Sync + 'static,
            {
                let mut nats_service = Service::new(nats_client);
                #(#handler_statements)*
                nats_service
            }

            pub fn client(nats_client: async_nats::Client, client: &'static str) -> impl #trait_ident {
                struct Client {
                    client: ServiceClient,
                }
                impl #trait_ident for Client {
                    #(#client_methods)*
                }
                Client { client: ServiceClient::new(nats_client, client) }
            }
        }
    })
}

struct MethodModel {
    method_ident: Ident,
    subject: LitStr,
    request_type: Option<Type>,
    response_type: Type,
}

fn parse_method(method: &TraitItemFn) -> Result<MethodModel> {
    let method_ident = method.sig.ident.clone();

    if method.sig.asyncness.is_none() {
        return Err(Error::new_spanned(method, "method must be `async fn`"));
    }

    let mut subject = None;
    for attr in &method.attrs {
        if attr.path().is_ident("subject") {
            subject = Some(parse_subject(attr)?);
        }
    }
    let subject = subject.ok_or_else(|| Error::new_spanned(method, r#"missing #[subject = "..."] attribute"#))?;

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

    Ok(MethodModel { method_ident, subject, request_type, response_type })
}

fn parse_subject(attr: &Attribute) -> Result<LitStr> {
    match &attr.meta {
        Meta::NameValue(name_value) => {
            if let Expr::Lit(ExprLit { lit: Lit::Str(subject), .. }) = &name_value.value {
                Ok(subject.clone())
            } else {
                Err(Error::new_spanned(attr, r#"expected #[subject = "..."]"#))
            }
        }
        Meta::List(_) => attr.parse_args::<LitStr>(),
        Meta::Path(_) => Err(Error::new_spanned(attr, r#"expected #[subject = "..."]"#)),
    }
}

fn build_handler_statement(model: &MethodModel) -> TokenStream {
    let method_ident = &model.method_ident;
    let subject = &model.subject;
    let fn_format = format!("{{}}::{method_ident}");

    let handler = if let Some(request_type) = &model.request_type {
        quote! {
            move |request: #request_type| {
                let svc = Arc::clone(&svc);
                async move {
                    context!(fn = format!(#fn_format, std::any::type_name::<T>()));
                    svc.#method_ident(request).await
                }
            }
        }
    } else {
        quote! {
            move |(): ()| {
                let svc = Arc::clone(&svc);
                async move {
                    context!(fn = format!(#fn_format, std::any::type_name::<T>()));
                    svc.#method_ident().await
                }
            }
        }
    };

    quote! {
        let svc = Arc::clone(&service);
        nats_service.add_handler(#subject, #handler);
    }
}

fn build_client_method(model: &MethodModel) -> TokenStream {
    let method_ident = &model.method_ident;
    let response_type = &model.response_type;
    let subject = &model.subject;

    if let Some(request_type) = &model.request_type {
        quote! {
            async fn #method_ident(&self, request: #request_type) -> #response_type {
                self.client.request(#subject, &request).await
            }
        }
    } else {
        quote! {
            async fn #method_ident(&self) -> #response_type {
                self.client.request(#subject, &()).await
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use quote::quote;

    use super::build;

    #[test]
    fn build_nats_api() {
        let source = quote! {
            #[nats_api]
            pub trait UserService {
                #[subject = "api.user.get_user_by_id"]
                async fn get_user_by_id(&self, request: GetUserRequest) -> Result<GetUserResponse, Exception>;

                #[subject = "api.user.create_user"]
                async fn create_user(&self, request: CreateUserRequest) -> Result<CreateUserResponse, Exception>;
            }
        };

        let output = build(source).unwrap();

        assert_eq!(
            output.to_string(),
            quote! {
                #[nats_api]
                pub trait UserService {
                    fn get_user_by_id(&self, request: GetUserRequest) -> impl ::core::future::Future<Output = Result<GetUserResponse, Exception> > + Send;
                    fn create_user(&self, request: CreateUserRequest) -> impl ::core::future::Future<Output = Result<CreateUserResponse, Exception> > + Send;
                }

                pub mod user_service {
                    use std::sync::Arc;

                    use framework::context;
                    use framework_nats::async_nats;
                    use framework_nats::service::Service;
                    use framework_nats::service::ServiceClient;

                    use super::*;

                    pub fn service<T>(nats_client: async_nats::Client, service: Arc<T>) -> Service
                    where
                        T: UserService + Send + Sync + 'static,
                    {
                        let mut nats_service = Service::new(nats_client);
                        let svc = Arc::clone(&service);
                        nats_service.add_handler("api.user.get_user_by_id", move |request: GetUserRequest| {
                            let svc = Arc::clone(&svc);
                            async move {
                                context!(fn = format!("{}::get_user_by_id", std::any::type_name::<T>()));
                                svc.get_user_by_id(request).await
                            }
                        });
                        let svc = Arc::clone(&service);
                        nats_service.add_handler("api.user.create_user", move |request: CreateUserRequest| {
                            let svc = Arc::clone(&svc);
                            async move {
                                context!(fn = format!("{}::create_user", std::any::type_name::<T>()));
                                svc.create_user(request).await
                            }
                        });
                        nats_service
                    }

                    pub fn client(nats_client: async_nats::Client, client: &'static str) -> impl UserService {
                        struct Client {
                            client: ServiceClient,
                        }
                        impl UserService for Client {
                            async fn get_user_by_id(&self, request: GetUserRequest) -> Result<GetUserResponse, Exception> {
                                self.client.request("api.user.get_user_by_id", &request).await
                            }
                            async fn create_user(&self, request: CreateUserRequest) -> Result<CreateUserResponse, Exception> {
                                self.client.request("api.user.create_user", &request).await
                            }
                        }
                        Client { client: ServiceClient::new(nats_client, client) }
                    }
                }
            }
            .to_string()
        );
    }

    #[test]
    fn build_nats_api_with_optional() {
        let source = quote! {
            #[nats_api]
            pub trait UserService {
                #[subject = "api.user.get_all_users"]
                async fn get_all_users(&self) -> Result<GetAllUsersResponse, Exception>;

                #[subject("api.user.delete_user")]
                async fn delete_user(&self, request: DeleteUserRequest) -> Result<(), Exception>;
            }
        };

        let output = build(source).unwrap();

        assert_eq!(
            output.to_string(),
            quote! {
                #[nats_api]
                pub trait UserService {
                    fn get_all_users(&self) -> impl ::core::future::Future<Output = Result<GetAllUsersResponse, Exception> > + Send;
                    fn delete_user(&self, request: DeleteUserRequest) -> impl ::core::future::Future<Output = Result<(), Exception> > + Send;
                }

                pub mod user_service {
                    use std::sync::Arc;

                    use framework::context;
                    use framework_nats::async_nats;
                    use framework_nats::service::Service;
                    use framework_nats::service::ServiceClient;

                    use super::*;

                    pub fn service<T>(nats_client: async_nats::Client, service: Arc<T>) -> Service
                    where
                        T: UserService + Send + Sync + 'static,
                    {
                        let mut nats_service = Service::new(nats_client);
                        let svc = Arc::clone(&service);
                        nats_service.add_handler("api.user.get_all_users", move |(): ()| {
                            let svc = Arc::clone(&svc);
                            async move {
                                context!(fn = format!("{}::get_all_users", std::any::type_name::<T>()));
                                svc.get_all_users().await
                            }
                        });
                        let svc = Arc::clone(&service);
                        nats_service.add_handler("api.user.delete_user", move |request: DeleteUserRequest| {
                            let svc = Arc::clone(&svc);
                            async move {
                                context!(fn = format!("{}::delete_user", std::any::type_name::<T>()));
                                svc.delete_user(request).await
                            }
                        });
                        nats_service
                    }

                    pub fn client(nats_client: async_nats::Client, client: &'static str) -> impl UserService {
                        struct Client {
                            client: ServiceClient,
                        }
                        impl UserService for Client {
                            async fn get_all_users(&self) -> Result<GetAllUsersResponse, Exception> {
                                self.client.request("api.user.get_all_users", &()).await
                            }
                            async fn delete_user(&self, request: DeleteUserRequest) -> Result<(), Exception> {
                                self.client.request("api.user.delete_user", &request).await
                            }
                        }
                        Client { client: ServiceClient::new(nats_client, client) }
                    }
                }
            }
            .to_string()
        );
    }

    #[test]
    fn build_nats_api_without_subject() {
        let source = quote! {
            pub trait UserService {
                async fn get_user_by_id(&self, request: GetUserRequest) -> Result<GetUserResponse, Exception>;
            }
        };

        let error = build(source).unwrap_err();
        assert_eq!(error.to_string(), r#"missing #[subject = "..."] attribute"#);
    }
}
