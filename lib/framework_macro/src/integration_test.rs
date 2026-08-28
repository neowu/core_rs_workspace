use proc_macro2::TokenStream;
use quote::quote;
use syn::Error;
use syn::Ident;
use syn::ItemFn;
use syn::Result;
use syn::ReturnType;
use syn::Visibility;
use syn::parse2;

pub(crate) fn build(tokens: TokenStream) -> Result<TokenStream> {
    let mut function: ItemFn = parse2(tokens)?;

    if function.sig.asyncness.is_none() {
        return Err(Error::new_spanned(&function.sig, "integration test must be async fn"));
    }
    if !function.sig.inputs.is_empty() {
        return Err(Error::new_spanned(&function.sig.inputs, "integration test must not take any argument"));
    }
    if matches!(function.sig.output, ReturnType::Default) {
        return Err(Error::new_spanned(&function.sig, "integration test must return Result<(), Exception>"));
    }

    let attrs = function.attrs.clone();
    let vis = function.vis.clone();
    let ident = function.sig.ident.clone();
    let name = ident.to_string();

    // the body is kept in an inner fn with the original signature, so `?` and the return type report errors on the
    // original code
    function.attrs.clear();
    function.vis = Visibility::Inherited;
    function.sig.ident = Ident::new("__body", ident.span());

    Ok(quote! {
        #[::tokio::test]
        #(#attrs)*
        #vis async fn #ident() {
            #function

            let mut system = ::framework::system::System::init(env!("CARGO_PKG_NAME"));
            system.start_action_logger(::framework::log::ConsoleAppender);

            let result = ::framework::log::action("test", None, async {
                ::framework::context!(test = #name);
                ::framework::log::trace();
                __body().await
            })
            .await;

            // flushes the action log before the assertion below fails the test
            let _result = system.shutdown(::std::time::Duration::from_secs(5)).await;

            if let Err(e) = result {
                panic!("{} failed, error={e}", #name);
            }
        }
    })
}
