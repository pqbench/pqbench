//! `#[derive(Resource)]`: the `Resource` impl a v1 type needs.
//!
//! The name field carries the path, one segment per `/`, with `{id}` for a
//! placeholder; the last placeholder names the resource, so the noun is not
//! repeated. An optional `alias` adds the dotted shorthand:
//!
//! ```ignore
//! #[derive(Resource)]
//! pub struct Table {
//!     #[resource(
//!         name = "catalogs/{catalog}/schemas/{schema}/tables/{table}",
//!         alias = "{catalog}.{schema}.{table}",
//!     )]
//!     pub name: String,
//!     // …
//! }
//! ```
//!
//! The derive fills [`Resource::from_name`], `name`, and `name_mut`; the parse
//! itself is [`pqbench::v1::name::pattern_name`], so the dotted shorthand and
//! the canonical spelling live there, not in the macro.

use proc_macro::TokenStream;
use quote::quote;
use syn::{Data, DeriveInput, Fields, LitStr, Type};

/// A v1 resource: its noun and its name pattern.
#[proc_macro_derive(Resource, attributes(resource))]
pub fn resource(input: TokenStream) -> TokenStream {
    let input = syn::parse_macro_input!(input as DeriveInput);
    expand(&input)
        .unwrap_or_else(syn::Error::into_compile_error)
        .into()
}

fn expand(input: &DeriveInput) -> syn::Result<proc_macro2::TokenStream> {
    if input
        .attrs
        .iter()
        .any(|attr| attr.path().is_ident("resource"))
    {
        return Err(syn::Error::new_spanned(
            input,
            "the noun is the name pattern's last placeholder",
        ));
    }
    let ident = &input.ident;
    let (field, pattern, alias) = name_field(input)?;
    let (segments, ids) = pattern_segments(&pattern)?;
    let noun = ids.last().expect("a pattern has an id");
    let alias = alias
        .map(|alias| alias_segments(&alias, &ids))
        .transpose()?
        .map(|alias| {
            let alias = (0..alias.len()).map(|_| String::new()).collect::<Vec<_>>();
            quote!(Some(&[#(#alias),*][..]))
        })
        .unwrap_or_else(|| quote!(None));
    let shape = segments
        .iter()
        .map(|segment| {
            if segment.is_empty() {
                "…".to_owned()
            } else {
                segment.clone()
            }
        })
        .collect::<Vec<_>>()
        .join("/");
    let placeholders = segments.iter().filter(|segment| segment.is_empty()).count();
    let ids = (0..placeholders)
        .map(syn::Index::from)
        .map(|index| quote!(ids[#index].clone()))
        .collect::<Vec<_>>();
    let (parts_type, parts) = if placeholders == 1 {
        (quote!(String), quote!(ids[0].clone()))
    } else {
        let types = (0..placeholders)
            .map(|_| quote!(String))
            .collect::<Vec<_>>();
        (quote!((#(#types),*)), quote!((#(#ids),*)))
    };
    let (impl_generics, ty_generics, where_clause) = input.generics.split_for_impl();
    Ok(quote! {
        impl #impl_generics #ident #ty_generics #where_clause {
            /// The ids the name spells, in path order: the resource name or
            /// its dotted shorthand.
            pub fn parse_name(value: &str) -> Option<#parts_type> {
                let ids = ::pqbench::v1::name::ids(value, &[#(#segments),*], #alias)?;
                Some(#parts)
            }
        }

        impl #impl_generics ::pqbench::v1::Resource for #ident #ty_generics #where_clause {
            const NOUN: &'static str = #noun;
            const SHAPE: &'static str = #shape;

            fn from_name(value: &str) -> Option<Self> {
                Some(Self {
                    #field: ::pqbench::v1::name::pattern_name(value, &[#(#segments),*], #alias)?,
                    ..Self::default()
                })
            }

            fn name(&self) -> &str {
                &self.#field
            }

            fn name_mut(&mut self) -> &mut String {
                &mut self.#field
            }
        }
    })
}

/// The field the name pattern is on, the pattern, and the optional alias.
fn name_field(input: &DeriveInput) -> syn::Result<(syn::Ident, LitStr, Option<LitStr>)> {
    let Data::Struct(data) = &input.data else {
        return Err(syn::Error::new_spanned(input, "Resource is for structs"));
    };
    let Fields::Named(fields) = &data.fields else {
        return Err(syn::Error::new_spanned(
            &data.fields,
            "Resource needs named fields",
        ));
    };
    let mut found = None;
    for field in &fields.named {
        let mut pattern = None;
        let mut alias = None;
        for attr in &field.attrs {
            if !attr.path().is_ident("resource") {
                continue;
            }
            attr.parse_nested_meta(|meta| {
                if meta.path.is_ident("name") {
                    pattern = Some(meta.value()?.parse()?);
                } else if meta.path.is_ident("alias") {
                    if alias.is_some() {
                        return Err(meta.error("one alias per name"));
                    }
                    alias = Some(meta.value()?.parse()?);
                } else {
                    return Err(meta.error("expected `name` or `alias`"));
                }
                Ok(())
            })?;
        }
        let Some(pattern) = pattern else {
            continue;
        };
        if found.is_some() {
            return Err(syn::Error::new_spanned(
                field,
                "only one field carries the name pattern",
            ));
        }
        if !is_string(&field.ty) {
            return Err(syn::Error::new_spanned(
                &field.ty,
                "the name field must be a `String`",
            ));
        }
        found = Some((field.ident.clone().expect("named field"), pattern, alias));
    }
    found.ok_or_else(|| {
        syn::Error::new_spanned(
            input,
            "a resource needs `#[resource(name = \"…\")]` on its name field",
        )
    })
}

fn is_string(ty: &Type) -> bool {
    let Type::Path(path) = ty else {
        return false;
    };
    path.path
        .segments
        .last()
        .is_some_and(|segment| segment.ident == "String")
}

/// The path segments (a literal, or `""` for a placeholder) and the placeholder
/// ids, in order.
fn pattern_segments(pattern: &LitStr) -> syn::Result<(Vec<String>, Vec<String>)> {
    let mut segments = Vec::new();
    let mut ids = Vec::new();
    for segment in pattern.value().split('/') {
        if segment.is_empty() {
            return Err(syn::Error::new(
                pattern.span(),
                "a name pattern has an empty segment",
            ));
        }
        if let Some(id) = segment
            .strip_prefix('{')
            .and_then(|rest| rest.strip_suffix('}'))
        {
            if id.is_empty() || !id.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') {
                return Err(syn::Error::new(
                    pattern.span(),
                    "a name placeholder is `{id}` with an alphanumeric id",
                ));
            }
            ids.push(id.to_owned());
            segments.push(String::new());
        } else if segment.contains('{') || segment.contains('}') {
            return Err(syn::Error::new(
                pattern.span(),
                "a name placeholder is a whole segment",
            ));
        } else {
            segments.push(segment.to_owned());
        }
    }
    if ids.is_empty() {
        return Err(syn::Error::new(
            pattern.span(),
            "a name pattern has no placeholder",
        ));
    }
    Ok((segments, ids))
}

/// The alias's ids, which must be the pattern's ids in order.
fn alias_segments(alias: &LitStr, ids: &[String]) -> syn::Result<Vec<String>> {
    let mut alias_ids = Vec::new();
    for segment in alias.value().split('.') {
        let Some(id) = segment
            .strip_prefix('{')
            .and_then(|rest| rest.strip_suffix('}'))
        else {
            return Err(syn::Error::new(
                alias.span(),
                "an alias is dot-separated placeholders",
            ));
        };
        alias_ids.push(id.to_owned());
    }
    if alias_ids != ids {
        return Err(syn::Error::new(
            alias.span(),
            "an alias names the pattern's ids in order",
        ));
    }
    Ok(alias_ids)
}
