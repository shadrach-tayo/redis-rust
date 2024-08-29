extern crate proc_macro;
extern crate quote;
extern crate syn;

use proc_macro::TokenStream;
use quote::quote;

use proc_macro2::Span;

#[proc_macro]
pub fn gen_cursor_util(input: TokenStream) -> TokenStream {
    // parse the string representation
    let name = input.to_string();

    // convert name string to Identifier
    let fn_name = syn::Ident::new(name.as_str(), Span::call_site());

    // extract string format for the return type
    let fn_type_str: Vec<&str> = name.split("_").collect();
    // convert vec['get', 'i8'] to LitStr('i8')
    let type_ident: syn::LitStr = syn::LitStr::new(fn_type_str[1], Span::call_site());

    // extract the util return type from the name string
    // convert vec['get', 'i8'] to Ident('i8')
    let return_type = syn::Ident::new(fn_type_str[1], Span::call_site());

    let expanded = quote! {
        fn #fn_name(src: &mut Cursor<&[u8]>) -> Result<#return_type> {
            if !src.has_remaining() {
                return Err(format!("Invalid {}", #type_ident).into());
            }

            Ok(src.#fn_name())
        }
    };

    TokenStream::from(expanded)
}
