use quote::quote;

use crate::FieldInfo;

pub(crate) fn from_row_impl(
    struct_name: &syn::Ident,
    fields: &Vec<(&syn::Field, FieldInfo)>,
) -> proc_macro2::TokenStream {
    // From<Row>: map all fields from their column names
    let from_assignments = fields.iter().map(|(field, info)| {
        let fname = &field.ident;
        let col = &info.column;
        quote! { #fname: row.get(#col), }
    });

    let from_row_impl = quote! {
        impl From<framework::db::Row> for #struct_name {
            fn from(row: framework::db::Row) -> #struct_name {
                #struct_name {
                    #(#from_assignments)*
                }
            }
        }
    };
    from_row_impl
}
