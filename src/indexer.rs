use anyhow::{Context, Result};
use rayon::prelude::*;
use std::collections::HashSet;
use std::fs;
use std::path::{Path, PathBuf};
use syn::{
    visit::Visit, Attribute, Fields, File, ImplItem, Item, ItemConst, ItemEnum, ItemImpl,
    ItemMacro, ItemStatic, ItemStruct, ItemTrait, ItemType, Signature, TraitItem, UseTree,
    Visibility,
};
use walkdir::WalkDir;

use crate::storage::{
    ConstantInfo, CrateItems, EnumInfo, FieldInfo, FunctionInfo, ImplInfo, MacroInfo,
    StructInfo, TraitInfo, TypeAliasInfo, VariantInfo,
};

pub struct IndexResult {
    pub items: CrateItems,
    pub reexported_crates: Vec<String>,
}

pub fn index_crate(crate_path: &Path, crate_name: &str) -> Result<IndexResult> {
    // Parse Cargo.toml to get actual dependencies
    let dependencies = parse_cargo_dependencies(crate_path);

    // Collect all .rs files first
    let files: Vec<(PathBuf, String)> = WalkDir::new(crate_path)
        .into_iter()
        .filter_map(|e| e.ok())
        .filter(|e| e.path().extension().is_some_and(|ext| ext == "rs"))
        .map(|entry| {
            let file_path = entry.path().to_path_buf();
            let relative_path = file_path
                .strip_prefix(crate_path)
                .unwrap_or(&file_path)
                .to_string_lossy()
                .to_string();
            (file_path, relative_path)
        })
        .collect();

    // Process files in parallel
    let results: Vec<_> = files
        .par_iter()
        .filter_map(|(file_path, relative_path)| {
            match index_file(file_path, relative_path, crate_name) {
                Ok(result) => Some(result),
                Err(e) => {
                    eprintln!("Warning: Failed to parse {:?}: {}", file_path, e);
                    None
                }
            }
        })
        .collect();

    // Merge results
    let mut items = CrateItems::default();
    let mut reexported_modules = HashSet::new();

    for (file_items, file_reexports) in results {
        items.functions.extend(file_items.functions);
        items.structs.extend(file_items.structs);
        items.enums.extend(file_items.enums);
        items.traits.extend(file_items.traits);
        items.macros.extend(file_items.macros);
        items.type_aliases.extend(file_items.type_aliases);
        items.constants.extend(file_items.constants);
        items.impls.extend(file_items.impls);
        reexported_modules.extend(file_reexports);
    }

    // Filter re-exports to only include actual dependencies
    let reexported_crates: Vec<String> = reexported_modules
        .into_iter()
        .filter(|module| dependencies.contains(module))
        .collect();

    Ok(IndexResult {
        items,
        reexported_crates,
    })
}

fn parse_cargo_dependencies(crate_path: &Path) -> HashSet<String> {
    let cargo_path = crate_path.join("Cargo.toml");
    let mut deps = HashSet::new();

    if let Ok(content) = fs::read_to_string(&cargo_path) {
        if let Ok(toml) = content.parse::<toml::Table>() {
            for section in ["dependencies", "dev-dependencies", "build-dependencies"] {
                if let Some(dependencies) = toml.get(section).and_then(|d| d.as_table()) {
                    for key in dependencies.keys() {
                        deps.insert(key.clone());
                    }
                }
            }
        }
    }

    deps
}

fn index_file(
    file_path: &Path,
    relative_path: &str,
    crate_name: &str,
) -> Result<(CrateItems, Vec<String>)> {
    let content = fs::read_to_string(file_path)
        .with_context(|| format!("Failed to read file {:?}", file_path))?;

    let syntax: File = syn::parse_file(&content)
        .with_context(|| format!("Failed to parse {:?}", file_path))?;

    let mut visitor = ItemVisitor {
        items: CrateItems::default(),
        file_path: relative_path.to_string(),
        crate_name: crate_name.to_string(),
    };

    visitor.visit_file(&syntax);

    // Extract re-exported external crates
    let reexports = extract_reexports(&syntax);

    Ok((visitor.items, reexports))
}

fn extract_reexports(syntax: &File) -> Vec<String> {
    let mut crates = Vec::new();

    for item in &syntax.items {
        if let Item::Use(use_item) = item {
            if matches!(use_item.vis, Visibility::Public(_)) {
                extract_crate_from_use_tree(&use_item.tree, &mut crates);
            }
        }
    }

    crates
}

fn extract_crate_from_use_tree(tree: &UseTree, crates: &mut Vec<String>) {
    match tree {
        UseTree::Path(path) => {
            let ident = path.ident.to_string();
            if ident != "self" && ident != "super" && ident != "crate" {
                let crate_name = ident.replace('_', "-");
                crates.push(crate_name);
            }
        }
        UseTree::Name(name) => {
            let ident = name.ident.to_string();
            if ident != "self" && ident != "super" && ident != "crate" {
                let crate_name = ident.replace('_', "-");
                crates.push(crate_name);
            }
        }
        UseTree::Rename(rename) => {
            let ident = rename.ident.to_string();
            if ident != "self" && ident != "super" && ident != "crate" {
                let crate_name = ident.replace('_', "-");
                crates.push(crate_name);
            }
        }
        UseTree::Group(_) | UseTree::Glob(_) => {}
    }
}

struct ItemVisitor {
    items: CrateItems,
    file_path: String,
    crate_name: String,
}

impl ItemVisitor {
    fn generate_id(&self, name: &str, line: usize, kind: &str) -> String {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let mut hasher = DefaultHasher::new();
        self.crate_name.hash(&mut hasher);
        self.file_path.hash(&mut hasher);
        name.hash(&mut hasher);
        line.hash(&mut hasher);
        kind.hash(&mut hasher);

        // Use full 64-bit hash for lower collision probability
        format!("{:016x}", hasher.finish())[..8].to_string()
    }

    fn visibility_str(vis: &Visibility) -> String {
        match vis {
            Visibility::Public(_) => "pub".to_string(),
            Visibility::Restricted(r) => format!("pub({})", quote::quote!(#r)),
            Visibility::Inherited => "private".to_string(),
        }
    }

    fn add_function(&mut self, sig: &Signature, attrs: &[Attribute], start_line: usize, end_line: Option<usize>) {
        let signature = format_signature(sig);
        let docs = extract_docs(attrs);
        let name = sig.ident.to_string();
        let id = self.generate_id(&name, start_line, "fn");

        self.items.functions.push(FunctionInfo {
            id,
            name,
            file: self.file_path.clone(),
            line: start_line,
            end_line,
            signature,
            docs,
        });
    }

    fn add_struct(&mut self, item: &ItemStruct) {
        let start_line = item.struct_token.span.start().line;
        let end_line = match &item.fields {
            Fields::Named(f) => Some(f.brace_token.span.close().end().line),
            Fields::Unnamed(f) => Some(f.paren_token.span.close().end().line),
            Fields::Unit => None,
        };

        let fields: Vec<FieldInfo> = match &item.fields {
            Fields::Named(named) => named
                .named
                .iter()
                .map(|f| {
                    let ty = &f.ty;
                    FieldInfo {
                        name: f.ident.as_ref().map(|i| i.to_string()).unwrap_or_default(),
                        type_str: quote::quote!(#ty).to_string(),
                        visibility: Self::visibility_str(&f.vis),
                        docs: extract_docs(&f.attrs),
                    }
                })
                .collect(),
            Fields::Unnamed(unnamed) => unnamed
                .unnamed
                .iter()
                .enumerate()
                .map(|(i, f)| {
                    let ty = &f.ty;
                    FieldInfo {
                        name: i.to_string(),
                        type_str: quote::quote!(#ty).to_string(),
                        visibility: Self::visibility_str(&f.vis),
                        docs: extract_docs(&f.attrs),
                    }
                })
                .collect(),
            Fields::Unit => vec![],
        };

        let name = item.ident.to_string();
        self.items.structs.push(StructInfo {
            id: self.generate_id(&name, start_line, "struct"),
            name,
            file: self.file_path.clone(),
            line: start_line,
            end_line,
            visibility: Self::visibility_str(&item.vis),
            fields,
            docs: extract_docs(&item.attrs),
        });
    }

    fn add_enum(&mut self, item: &ItemEnum) {
        let start_line = item.enum_token.span.start().line;
        let end_line = Some(item.brace_token.span.close().end().line);

        let variants: Vec<VariantInfo> = item
            .variants
            .iter()
            .map(|v| {
                let (kind, fields) = match &v.fields {
                    Fields::Unit => ("unit".to_string(), None),
                    Fields::Unnamed(f) => {
                        let types: Vec<String> = f
                            .unnamed
                            .iter()
                            .map(|field| {
                                let ty = &field.ty;
                                quote::quote!(#ty).to_string()
                            })
                            .collect();
                        ("tuple".to_string(), Some(types.join(", ")))
                    }
                    Fields::Named(f) => {
                        let fields_str: Vec<String> = f
                            .named
                            .iter()
                            .map(|field| {
                                let name = field.ident.as_ref().map(|i| i.to_string()).unwrap_or_default();
                                let ty = &field.ty;
                                format!("{}: {}", name, quote::quote!(#ty))
                            })
                            .collect();
                        ("struct".to_string(), Some(fields_str.join(", ")))
                    }
                };
                VariantInfo {
                    name: v.ident.to_string(),
                    kind,
                    fields,
                    docs: extract_docs(&v.attrs),
                }
            })
            .collect();

        let name = item.ident.to_string();
        self.items.enums.push(EnumInfo {
            id: self.generate_id(&name, start_line, "enum"),
            name,
            file: self.file_path.clone(),
            line: start_line,
            end_line,
            visibility: Self::visibility_str(&item.vis),
            variants,
            docs: extract_docs(&item.attrs),
        });
    }

    fn add_trait(&mut self, item: &ItemTrait) {
        let start_line = item.trait_token.span.start().line;
        let end_line = Some(item.brace_token.span.close().end().line);

        let name = item.ident.to_string();
        self.items.traits.push(TraitInfo {
            id: self.generate_id(&name, start_line, "trait"),
            name,
            file: self.file_path.clone(),
            line: start_line,
            end_line,
            visibility: Self::visibility_str(&item.vis),
            docs: extract_docs(&item.attrs),
        });
    }

    fn add_macro(&mut self, item: &ItemMacro) {
        if let Some(ident) = &item.ident {
            let start_line = item.mac.path.segments.first()
                .map(|s| s.ident.span().start().line)
                .unwrap_or(1);
            let name = ident.to_string();

            self.items.macros.push(MacroInfo {
                id: self.generate_id(&name, start_line, "macro"),
                name,
                file: self.file_path.clone(),
                line: start_line,
                end_line: None,
                kind: "declarative".to_string(),
                docs: extract_docs(&item.attrs),
            });
        }
    }

    fn add_type_alias(&mut self, item: &ItemType) {
        let start_line = item.type_token.span.start().line;
        let ty = &item.ty;
        let name = item.ident.to_string();

        self.items.type_aliases.push(TypeAliasInfo {
            id: self.generate_id(&name, start_line, "type"),
            name,
            file: self.file_path.clone(),
            line: start_line,
            type_str: quote::quote!(#ty).to_string(),
            visibility: Self::visibility_str(&item.vis),
            docs: extract_docs(&item.attrs),
        });
    }

    fn add_const(&mut self, item: &ItemConst) {
        let start_line = item.const_token.span.start().line;
        let ty = &item.ty;
        let name = item.ident.to_string();

        self.items.constants.push(ConstantInfo {
            id: self.generate_id(&name, start_line, "const"),
            name,
            file: self.file_path.clone(),
            line: start_line,
            kind: "const".to_string(),
            type_str: quote::quote!(#ty).to_string(),
            visibility: Self::visibility_str(&item.vis),
            docs: extract_docs(&item.attrs),
        });
    }

    fn add_static(&mut self, item: &ItemStatic) {
        let start_line = item.static_token.span.start().line;
        let ty = &item.ty;
        let name = item.ident.to_string();

        self.items.constants.push(ConstantInfo {
            id: self.generate_id(&name, start_line, "static"),
            name,
            file: self.file_path.clone(),
            line: start_line,
            kind: "static".to_string(),
            type_str: quote::quote!(#ty).to_string(),
            visibility: Self::visibility_str(&item.vis),
            docs: extract_docs(&item.attrs),
        });
    }

    fn add_impl(&mut self, item: &ItemImpl) {
        let start_line = item.impl_token.span.start().line;
        let end_line = Some(item.brace_token.span.close().end().line);

        let self_ty = &item.self_ty;
        let self_type = quote::quote!(#self_ty).to_string();
        let trait_name = item.trait_.as_ref().map(|(_, path, _)| {
            quote::quote!(#path).to_string()
        });

        // Use self_type + trait_name for ID uniqueness
        let id_name = match &trait_name {
            Some(t) => format!("{}_{}", self_type, t),
            None => self_type.clone(),
        };

        self.items.impls.push(ImplInfo {
            id: self.generate_id(&id_name, start_line, "impl"),
            file: self.file_path.clone(),
            line: start_line,
            end_line,
            self_type,
            trait_name,
        });
    }
}

impl<'ast> Visit<'ast> for ItemVisitor {
    fn visit_item(&mut self, item: &'ast Item) {
        match item {
            Item::Fn(func) => {
                let start = func.sig.fn_token.span.start().line;
                let end = func.block.brace_token.span.close().end().line;
                self.add_function(&func.sig, &func.attrs, start, Some(end));
            }
            Item::Struct(s) => self.add_struct(s),
            Item::Enum(e) => self.add_enum(e),
            Item::Trait(t) => self.add_trait(t),
            Item::Macro(m) => self.add_macro(m),
            Item::Type(t) => self.add_type_alias(t),
            Item::Const(c) => self.add_const(c),
            Item::Static(s) => self.add_static(s),
            Item::Impl(i) => self.add_impl(i),
            _ => {}
        }
        syn::visit::visit_item(self, item);
    }

    fn visit_impl_item(&mut self, item: &'ast ImplItem) {
        if let ImplItem::Fn(method) = item {
            let start = method.sig.fn_token.span.start().line;
            let end = method.block.brace_token.span.close().end().line;
            self.add_function(&method.sig, &method.attrs, start, Some(end));
        }
        syn::visit::visit_impl_item(self, item);
    }

    fn visit_trait_item(&mut self, item: &'ast TraitItem) {
        if let TraitItem::Fn(method) = item {
            let start = method.sig.fn_token.span.start().line;
            let end = method
                .default
                .as_ref()
                .map(|block| block.brace_token.span.close().end().line);
            self.add_function(&method.sig, &method.attrs, start, end);
        }
        syn::visit::visit_trait_item(self, item);
    }
}

fn extract_docs(attrs: &[Attribute]) -> Option<String> {
    let doc_lines: Vec<String> = attrs
        .iter()
        .filter_map(|attr| {
            if attr.path().is_ident("doc") {
                if let syn::Meta::NameValue(meta) = &attr.meta {
                    if let syn::Expr::Lit(expr_lit) = &meta.value {
                        if let syn::Lit::Str(lit_str) = &expr_lit.lit {
                            return Some(lit_str.value());
                        }
                    }
                }
            }
            None
        })
        .collect();

    if doc_lines.is_empty() {
        None
    } else {
        let docs = doc_lines
            .iter()
            .map(|line| line.strip_prefix(' ').unwrap_or(line))
            .collect::<Vec<_>>()
            .join("\n")
            .trim()
            .to_string();

        if docs.is_empty() {
            None
        } else {
            Some(docs)
        }
    }
}

fn format_signature(sig: &Signature) -> String {
    let asyncness = if sig.asyncness.is_some() { "async " } else { "" };
    let unsafety = if sig.unsafety.is_some() { "unsafe " } else { "" };
    let constness = if sig.constness.is_some() { "const " } else { "" };

    let generics = if sig.generics.params.is_empty() {
        String::new()
    } else {
        let params: Vec<String> = sig
            .generics
            .params
            .iter()
            .map(|p| quote::quote!(#p).to_string())
            .collect();
        format!("<{}>", params.join(", "))
    };

    let inputs: Vec<String> = sig
        .inputs
        .iter()
        .map(|arg| quote::quote!(#arg).to_string())
        .collect();

    let output = match &sig.output {
        syn::ReturnType::Default => String::new(),
        syn::ReturnType::Type(_, ty) => format!(" -> {}", quote::quote!(#ty)),
    };

    format!(
        "{}{}{}fn {}{}({}){}",
        constness, asyncness, unsafety, sig.ident, generics,
        inputs.join(", "),
        output
    )
}
