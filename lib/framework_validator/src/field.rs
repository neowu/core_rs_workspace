use syn::Attribute;
use syn::Field;
use syn::Ident;
use syn::Type;
use syn::punctuated::Punctuated;
use syn::token::Comma;

// only support struct with named fields
pub struct FieldDefinition<'a> {
    pub name: String,
    pub ident: &'a Option<Ident>,
    pub is_optional: bool,
    attrs: &'a Vec<Attribute>,
}

impl<'a> FieldDefinition<'a> {
    fn new(field: &'a Field) -> Self {
        let name = field
            .ident
            .as_ref()
            .map(|i| i.to_string())
            .expect("only support struct with named fields");

        let ident = &field.ident;

        let is_optional = if let Type::Path(path) = &field.ty {
            path.path.segments.iter().any(|segment| segment.ident == "Option")
        } else {
            false
        };

        let attrs = &field.attrs;

        FieldDefinition {
            name,
            ident,
            is_optional,
            attrs,
        }
    }

    pub fn attr(&self, ident: &str) -> Option<&Attribute> {
        self.attrs.iter().find(|&attr| attr.path().is_ident(ident))
    }
}

pub fn parse<'a>(fields: &'a Punctuated<Field, Comma>) -> Vec<FieldDefinition<'a>> {
    fields.iter().map(FieldDefinition::new).collect()
}
