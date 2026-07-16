use proc_macro2::Literal;
use proc_macro2::TokenStream;
use quote::quote;
use syn::Data;
use syn::DeriveInput;
use syn::Error;
use syn::Expr;
use syn::ExprLit;
use syn::ExprUnary;
use syn::Fields;
use syn::Lit;
use syn::Result;
use syn::UnOp;

pub(crate) fn build(tokens: TokenStream) -> Result<TokenStream> {
    let input: DeriveInput = syn::parse2(tokens)?;
    let Data::Enum(data) = &input.data else {
        return Err(Error::new_spanned(&input.ident, "#[derive(Enum8)] only supports enum"));
    };
    let ident = &input.ident;

    let mut variants = vec![];
    let mut values = vec![];
    for variant in &data.variants {
        if !matches!(variant.fields, Fields::Unit) {
            return Err(Error::new_spanned(variant, "enum variant must not have fields"));
        }
        let Some((_, discriminant)) = &variant.discriminant else {
            return Err(Error::new_spanned(variant, "enum variant must have explicit discriminant, e.g. `Ok = 1`"));
        };
        variants.push(&variant.ident);
        values.push(parse_discriminant(discriminant)?);
    }

    Ok(quote! {
        impl serde::Serialize for #ident {
            fn serialize<S: serde::Serializer>(&self, serializer: S) -> ::std::result::Result<S::Ok, S::Error> {
                serializer.serialize_i8(match self {
                    #(Self::#variants => #values,)*
                })
            }
        }

        impl<'de> serde::Deserialize<'de> for #ident {
            fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> ::std::result::Result<Self, D::Error> {
                let value = <i8 as serde::Deserialize>::deserialize(deserializer)?;
                match value {
                    #(#values => Ok(Self::#variants),)*
                    _ => Err(serde::de::Error::custom(format!("unknown enum value, value={value}"))),
                }
            }
        }
    })
}

// clickhouse Enum8 values are Int8, so the discriminant must fit in i8
fn parse_discriminant(expr: &Expr) -> Result<Literal> {
    // parsed as i16 so `-128` (`128` alone overflows i8) parses, then range checked below
    let value = if let Expr::Lit(ExprLit { lit: Lit::Int(lit), .. }) = expr {
        lit.base10_parse::<i16>()?
    } else if let Expr::Unary(ExprUnary { op: UnOp::Neg(_), expr: inner, .. }) = expr
        && let Expr::Lit(ExprLit { lit: Lit::Int(lit), .. }) = inner.as_ref()
    {
        -lit.base10_parse::<i16>()?
    } else {
        return Err(Error::new_spanned(expr, "enum discriminant must be an integer literal"));
    };
    let value = i8::try_from(value).map_err(|_err| Error::new_spanned(expr, "enum discriminant must be within Enum8 range (-128..=127)"))?;
    Ok(Literal::i8_unsuffixed(value))
}

#[cfg(test)]
mod tests {
    use quote::quote;

    use super::build;

    #[test]
    fn build_enum8() {
        let source = quote! {
            #[derive(Enum8)]
            enum ActionResult {
                Ok = 1,
                Warn = 2,
                Error = -3,
            }
        };

        let output = build(source).unwrap();

        assert_eq!(
            output.to_string(),
            quote! {
                impl serde::Serialize for ActionResult {
                    fn serialize<S: serde::Serializer>(&self, serializer: S) -> ::std::result::Result<S::Ok, S::Error> {
                        serializer.serialize_i8(match self {
                            Self::Ok => 1,
                            Self::Warn => 2,
                            Self::Error => -3,
                        })
                    }
                }

                impl<'de> serde::Deserialize<'de> for ActionResult {
                    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> ::std::result::Result<Self, D::Error> {
                        let value = <i8 as serde::Deserialize>::deserialize(deserializer)?;
                        match value {
                            1 => Ok(Self::Ok),
                            2 => Ok(Self::Warn),
                            -3 => Ok(Self::Error),
                            _ => Err(serde::de::Error::custom(format!("unknown enum value, value={value}"))),
                        }
                    }
                }
            }
            .to_string()
        );
    }

    #[test]
    fn build_enum8_with_struct() {
        let source = quote! {
            struct ActionResult {
                result: i8,
            }
        };
        let err = build(source).unwrap_err();
        assert_eq!(err.to_string(), "#[derive(Enum8)] only supports enum");
    }

    #[test]
    fn build_enum8_with_variant_fields() {
        let source = quote! {
            enum ActionResult {
                Ok(String) = 1,
            }
        };
        let err = build(source).unwrap_err();
        assert_eq!(err.to_string(), "enum variant must not have fields");
    }

    #[test]
    fn build_enum8_without_discriminant() {
        let source = quote! {
            enum ActionResult {
                Ok = 1,
                Warn,
            }
        };
        let err = build(source).unwrap_err();
        assert_eq!(err.to_string(), "enum variant must have explicit discriminant, e.g. `Ok = 1`");
    }

    #[test]
    fn build_enum8_with_overflow_discriminant() {
        let source = quote! {
            enum ActionResult {
                Ok = 128,
            }
        };
        let err = build(source).unwrap_err();
        assert_eq!(err.to_string(), "enum discriminant must be within Enum8 range (-128..=127)");
    }

    #[test]
    fn build_enum8_with_underflow_discriminant() {
        let source = quote! {
            enum ActionResult {
                Ok = -129,
            }
        };
        let err = build(source).unwrap_err();
        assert_eq!(err.to_string(), "enum discriminant must be within Enum8 range (-128..=127)");
    }
}
