//! `#[derive(Resource)]`: the `Parent` impl a v1 resource needs.
//!
//! The name field carries the path, one segment per `/`, with `{id}` for a
//! placeholder; the last placeholder names the resource, so the noun is not
//! repeated:
//!
//! ```ignore
//! #[derive(Resource)]
//! pub struct Table {
//!     #[resource(name = "catalogs/{catalog}/schemas/{schema}/tables/{table}")]
//!     pub name: String,
//!     // …
//! }
//! ```
//!
//! The derive fills [`Parent::from_name`], `name`, and `name_mut`; the parse
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
    let (field, pattern) = name_field(input)?;
    let (segments, noun) = pattern_segments(&pattern)?;
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
                let ids = ::pqbench::v1::name::ids(value, &[#(#segments),*])?;
                Some(#parts)
            }
        }

        impl #impl_generics ::pqbench::v1::Parent for #ident #ty_generics #where_clause {
            const NOUN: &'static str = #noun;
            const SHAPE: &'static str = #shape;

            fn from_name(value: &str) -> Option<Self> {
                Some(Self {
                    #field: ::pqbench::v1::name::pattern_name(value, &[#(#segments),*])?,
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

/// The field the name pattern is on, and the pattern.
fn name_field(input: &DeriveInput) -> syn::Result<(syn::Ident, LitStr)> {
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
        for attr in &field.attrs {
            if !attr.path().is_ident("resource") {
                continue;
            }
            attr.parse_nested_meta(|meta| {
                if !meta.path.is_ident("name") {
                    return Err(meta.error("expected `name = \"…\"`"));
                }
                pattern = Some(meta.value()?.parse()?);
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
        found = Some((field.ident.clone().expect("named field"), pattern));
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

/// The path segments (a literal, or `""` for a placeholder) and the noun: the
/// last placeholder's name.
fn pattern_segments(pattern: &LitStr) -> syn::Result<(Vec<String>, String)> {
    let mut segments = Vec::new();
    let mut noun = None;
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
            noun = Some(id.to_owned());
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
    let Some(noun) = noun else {
        return Err(syn::Error::new(
            pattern.span(),
            "a name pattern has no placeholder",
        ));
    };
    Ok((segments, noun))
}
