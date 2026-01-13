mod embeddings;
mod fetcher;
mod indexer;
mod mcp;
mod search;
mod storage;

use anyhow::Result;
use clap::{Parser, Subcommand};
use rayon::prelude::*;
use regex::Regex;
use std::collections::HashSet;

use crate::embeddings::{embedding_to_bytes, EmbeddingManager};
use crate::fetcher::Fetcher;
use crate::indexer::index_crate;
use crate::search::{search_functions, search_regex};
use crate::storage::{
    ConstantInfo, Database, EnumInfo, ImplInfo, MacroInfo, StructInfo, TraitInfo, TypeAliasInfo,
};

#[derive(Parser)]
#[command(name = "crate-indexer")]
#[command(about = "Index and search Rust crates from crates.io")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Fetch and index a crate from crates.io
    Fetch {
        /// Name of the crate to fetch
        crate_name: String,
        /// Specific version to fetch (defaults to latest)
        #[arg(short, long)]
        version: Option<String>,
    },
    /// Search a crate with a regex pattern
    Search {
        /// Name of the crate to search (e.g., "anyhow" or "anyhow-1.0.100")
        crate_name: String,
        /// Regex pattern to search for
        pattern: String,
    },
    /// List or search function definitions in a crate
    Functions {
        /// Name of the crate to search (e.g., "anyhow" or "anyhow-1.0.100")
        crate_name: String,
        /// Optional regex pattern to filter functions
        pattern: Option<String>,
    },
    /// List or search struct definitions in a crate
    Structs {
        /// Name of the crate to search
        crate_name: String,
        /// Optional regex pattern to filter structs
        pattern: Option<String>,
    },
    /// List or search enum definitions in a crate
    Enums {
        /// Name of the crate to search
        crate_name: String,
        /// Optional regex pattern to filter enums
        pattern: Option<String>,
    },
    /// List or search trait definitions in a crate
    Traits {
        /// Name of the crate to search
        crate_name: String,
        /// Optional regex pattern to filter traits
        pattern: Option<String>,
    },
    /// List or search macro definitions in a crate
    Macros {
        /// Name of the crate to search
        crate_name: String,
        /// Optional regex pattern to filter macros
        pattern: Option<String>,
    },
    /// List or search type alias definitions in a crate
    Types {
        /// Name of the crate to search
        crate_name: String,
        /// Optional regex pattern to filter type aliases
        pattern: Option<String>,
    },
    /// List or search constant/static definitions in a crate
    Consts {
        /// Name of the crate to search
        crate_name: String,
        /// Optional regex pattern to filter constants
        pattern: Option<String>,
    },
    /// List or search impl blocks in a crate
    Impls {
        /// Name of the crate to search
        crate_name: String,
        /// Optional regex pattern to filter by type or trait name
        pattern: Option<String>,
    },
    /// Show full details of an item by ID, including source code
    Show {
        /// Item ID (8-character hex)
        id: String,
    },
    /// Get the latest version of a crate from crates.io
    Latest {
        /// Name of the crate
        crate_name: String,
    },
    /// Read a file from an indexed crate
    Read {
        /// Name of the crate (e.g., "anyhow" or "anyhow-1.0.100")
        crate_name: String,
        /// Path to file within the crate (e.g., "src/lib.rs")
        file_path: String,
        /// Start line number (1-indexed, default: 1)
        #[arg(short, long)]
        start: Option<usize>,
        /// End line number (1-indexed, default: end of file)
        #[arg(short, long)]
        end: Option<usize>,
    },
    /// Show the README of a crate
    Readme {
        /// Name of the crate (e.g., "anyhow" or "anyhow-1.0.100")
        crate_name: String,
    },
    /// Run as an MCP server (for AI assistant integration)
    Mcp,
    /// Semantic search within a crate using natural language
    SemanticSearch {
        /// Name of the crate to search
        crate_name: String,
        /// Natural language search query
        query: String,
        /// Maximum results (default 10)
        #[arg(short, long, default_value = "10")]
        limit: usize,
    },
    /// Generate embeddings for a crate (for semantic search)
    Embed {
        /// Name of the crate to embed
        crate_name: String,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Commands::Fetch { crate_name, version } => cmd_fetch(&crate_name, version.as_deref())?,
        Commands::Search { crate_name, pattern } => cmd_search(&crate_name, &pattern)?,
        Commands::Functions { crate_name, pattern } => cmd_functions(&crate_name, pattern.as_deref())?,
        Commands::Structs { crate_name, pattern } => cmd_structs(&crate_name, pattern.as_deref())?,
        Commands::Enums { crate_name, pattern } => cmd_enums(&crate_name, pattern.as_deref())?,
        Commands::Traits { crate_name, pattern } => cmd_traits(&crate_name, pattern.as_deref())?,
        Commands::Macros { crate_name, pattern } => cmd_macros(&crate_name, pattern.as_deref())?,
        Commands::Types { crate_name, pattern } => cmd_types(&crate_name, pattern.as_deref())?,
        Commands::Consts { crate_name, pattern } => cmd_consts(&crate_name, pattern.as_deref())?,
        Commands::Impls { crate_name, pattern } => cmd_impls(&crate_name, pattern.as_deref())?,
        Commands::Show { id } => cmd_show(&id)?,
        Commands::Latest { crate_name } => cmd_latest(&crate_name)?,
        Commands::Read { crate_name, file_path, start, end } => cmd_read(&crate_name, &file_path, start, end)?,
        Commands::Readme { crate_name } => cmd_readme(&crate_name)?,
        Commands::Mcp => {
            mcp::run_mcp_server().await?;
        }
        Commands::SemanticSearch { crate_name, query, limit } => {
            cmd_semantic_search(&crate_name, &query, limit).await?;
        }
        Commands::Embed { crate_name } => {
            cmd_embed(&crate_name).await?;
        }
    }

    Ok(())
}

fn cmd_fetch(crate_name: &str, version: Option<&str>) -> Result<()> {
    let db = Database::open()?;
    let before_count = db.list_crate_keys()?.len();
    fetch_single_crate(&db, crate_name, version)?;
    let after_count = db.list_crate_keys()?.len();
    println!("\nDone! Indexed {} crates total.", after_count - before_count);
    Ok(())
}

fn cmd_search(crate_name: &str, pattern: &str) -> Result<()> {
    let db = Database::open()?;

    let crate_keys = find_crate_keys_with_reexports(&db, crate_name)?;
    let mut total_matches = 0;

    for crate_key in &crate_keys {
        if let Some(crate_path) = db.get_crate_path(crate_key)? {
            let matches = search_regex(&crate_path, pattern)?;

            if !matches.is_empty() {
                if total_matches == 0 {
                    println!("Results for pattern '{}':\n", pattern);
                }
                println!("── {} ──", crate_key);
                for m in &matches {
                    println!("  {}:{}: {}", m.file, m.line, m.content);
                }
                println!();
                total_matches += matches.len();
            }
        }
    }

    if total_matches == 0 {
        println!("No matches found for pattern: {}", pattern);
    } else {
        println!("Total: {} matches across {} crate(s)", total_matches, crate_keys.len());
    }

    Ok(())
}

fn cmd_functions(crate_name: &str, pattern: Option<&str>) -> Result<()> {
    let db = Database::open()?;

    let crate_keys = find_crate_keys_with_reexports(&db, crate_name)?;
    let mut total_functions = 0;

    for crate_key in &crate_keys {
        let functions = db.get_functions(crate_key)?;
        let matches = search_functions(&functions, pattern)?;

        if !matches.is_empty() {
            println!("── {} ({} functions) ──\n", crate_key, matches.len());
            for func in &matches {
                println!("[{}] {}", func.id, func.signature);
                println!("  {}:{}", func.file, func.line);
                if let Some(docs) = &func.docs {
                    let first_line = docs.lines().next().unwrap_or("");
                    let truncated = if first_line.len() > 80 {
                        format!("{}...", &first_line[..77])
                    } else {
                        first_line.to_string()
                    };
                    println!("  /// {}", truncated);
                }
                println!();
            }
            total_functions += matches.len();
        }
    }

    if total_functions == 0 {
        if let Some(p) = pattern {
            println!("No functions matching '{}'", p);
        } else {
            println!("No functions found");
        }
    } else {
        println!("Total: {} functions across {} crate(s)", total_functions, crate_keys.len());
    }

    Ok(())
}

fn cmd_structs(crate_name: &str, pattern: Option<&str>) -> Result<()> {
    let db = Database::open()?;
    let regex = pattern.map(Regex::new).transpose()?;

    let crate_keys = find_crate_keys_with_reexports(&db, crate_name)?;
    let mut total = 0;

    for crate_key in &crate_keys {
        let structs = db.get_structs(crate_key)?;
        let matches: Vec<&StructInfo> = structs
            .iter()
            .filter(|s| regex.as_ref().map(|r| r.is_match(&s.name)).unwrap_or(true))
            .collect();

        if !matches.is_empty() {
            println!("── {} ({} structs) ──\n", crate_key, matches.len());
            for s in &matches {
                println!("[{}] {} struct {}", s.id, s.visibility, s.name);
                println!("  {}:{}", s.file, s.line);
                if !s.fields.is_empty() {
                    let field_names: Vec<&str> = s.fields.iter().take(5).map(|f| f.name.as_str()).collect();
                    let more = if s.fields.len() > 5 { format!(" +{} more", s.fields.len() - 5) } else { String::new() };
                    println!("  Fields: {}{}", field_names.join(", "), more);
                }
                if let Some(docs) = &s.docs {
                    let first_line = docs.lines().next().unwrap_or("");
                    let truncated = truncate_str(first_line, 80);
                    println!("  /// {}", truncated);
                }
                println!();
            }
            total += matches.len();
        }
    }

    print_summary("structs", total, pattern, crate_keys.len());
    Ok(())
}

fn cmd_enums(crate_name: &str, pattern: Option<&str>) -> Result<()> {
    let db = Database::open()?;
    let regex = pattern.map(Regex::new).transpose()?;

    let crate_keys = find_crate_keys_with_reexports(&db, crate_name)?;
    let mut total = 0;

    for crate_key in &crate_keys {
        let enums = db.get_enums(crate_key)?;
        let matches: Vec<&EnumInfo> = enums
            .iter()
            .filter(|e| regex.as_ref().map(|r| r.is_match(&e.name)).unwrap_or(true))
            .collect();

        if !matches.is_empty() {
            println!("── {} ({} enums) ──\n", crate_key, matches.len());
            for e in &matches {
                println!("[{}] {} enum {}", e.id, e.visibility, e.name);
                println!("  {}:{}", e.file, e.line);
                let variant_names: Vec<&str> = e.variants.iter().take(5).map(|v| v.name.as_str()).collect();
                let more = if e.variants.len() > 5 { format!(" +{} more", e.variants.len() - 5) } else { String::new() };
                println!("  Variants: {}{}", variant_names.join(", "), more);
                if let Some(docs) = &e.docs {
                    let first_line = docs.lines().next().unwrap_or("");
                    println!("  /// {}", truncate_str(first_line, 80));
                }
                println!();
            }
            total += matches.len();
        }
    }

    print_summary("enums", total, pattern, crate_keys.len());
    Ok(())
}

fn cmd_traits(crate_name: &str, pattern: Option<&str>) -> Result<()> {
    let db = Database::open()?;
    let regex = pattern.map(Regex::new).transpose()?;

    let crate_keys = find_crate_keys_with_reexports(&db, crate_name)?;
    let mut total = 0;

    for crate_key in &crate_keys {
        let traits = db.get_traits(crate_key)?;
        let matches: Vec<&TraitInfo> = traits
            .iter()
            .filter(|t| regex.as_ref().map(|r| r.is_match(&t.name)).unwrap_or(true))
            .collect();

        if !matches.is_empty() {
            println!("── {} ({} traits) ──\n", crate_key, matches.len());
            for t in &matches {
                println!("[{}] {} trait {}", t.id, t.visibility, t.name);
                println!("  {}:{}", t.file, t.line);
                if let Some(docs) = &t.docs {
                    let first_line = docs.lines().next().unwrap_or("");
                    println!("  /// {}", truncate_str(first_line, 80));
                }
                println!();
            }
            total += matches.len();
        }
    }

    print_summary("traits", total, pattern, crate_keys.len());
    Ok(())
}

fn cmd_macros(crate_name: &str, pattern: Option<&str>) -> Result<()> {
    let db = Database::open()?;
    let regex = pattern.map(Regex::new).transpose()?;

    let crate_keys = find_crate_keys_with_reexports(&db, crate_name)?;
    let mut total = 0;

    for crate_key in &crate_keys {
        let macros = db.get_macros(crate_key)?;
        let matches: Vec<&MacroInfo> = macros
            .iter()
            .filter(|m| regex.as_ref().map(|r| r.is_match(&m.name)).unwrap_or(true))
            .collect();

        if !matches.is_empty() {
            println!("── {} ({} macros) ──\n", crate_key, matches.len());
            for m in &matches {
                println!("[{}] {}! ({})", m.id, m.name, m.kind);
                println!("  {}:{}", m.file, m.line);
                if let Some(docs) = &m.docs {
                    let first_line = docs.lines().next().unwrap_or("");
                    println!("  /// {}", truncate_str(first_line, 80));
                }
                println!();
            }
            total += matches.len();
        }
    }

    print_summary("macros", total, pattern, crate_keys.len());
    Ok(())
}

fn cmd_types(crate_name: &str, pattern: Option<&str>) -> Result<()> {
    let db = Database::open()?;
    let regex = pattern.map(Regex::new).transpose()?;

    let crate_keys = find_crate_keys_with_reexports(&db, crate_name)?;
    let mut total = 0;

    for crate_key in &crate_keys {
        let types = db.get_type_aliases(crate_key)?;
        let matches: Vec<&TypeAliasInfo> = types
            .iter()
            .filter(|t| regex.as_ref().map(|r| r.is_match(&t.name)).unwrap_or(true))
            .collect();

        if !matches.is_empty() {
            println!("── {} ({} type aliases) ──\n", crate_key, matches.len());
            for t in &matches {
                println!("[{}] {} type {} = {}", t.id, t.visibility, t.name, truncate_str(&t.type_str, 60));
                println!("  {}:{}", t.file, t.line);
                if let Some(docs) = &t.docs {
                    let first_line = docs.lines().next().unwrap_or("");
                    println!("  /// {}", truncate_str(first_line, 80));
                }
                println!();
            }
            total += matches.len();
        }
    }

    print_summary("type aliases", total, pattern, crate_keys.len());
    Ok(())
}

fn cmd_consts(crate_name: &str, pattern: Option<&str>) -> Result<()> {
    let db = Database::open()?;
    let regex = pattern.map(Regex::new).transpose()?;

    let crate_keys = find_crate_keys_with_reexports(&db, crate_name)?;
    let mut total = 0;

    for crate_key in &crate_keys {
        let constants = db.get_constants(crate_key)?;
        let matches: Vec<&ConstantInfo> = constants
            .iter()
            .filter(|c| regex.as_ref().map(|r| r.is_match(&c.name)).unwrap_or(true))
            .collect();

        if !matches.is_empty() {
            println!("── {} ({} constants) ──\n", crate_key, matches.len());
            for c in &matches {
                println!("[{}] {} {} {}: {}", c.id, c.visibility, c.kind, c.name, truncate_str(&c.type_str, 50));
                println!("  {}:{}", c.file, c.line);
                if let Some(docs) = &c.docs {
                    let first_line = docs.lines().next().unwrap_or("");
                    println!("  /// {}", truncate_str(first_line, 80));
                }
                println!();
            }
            total += matches.len();
        }
    }

    print_summary("constants", total, pattern, crate_keys.len());
    Ok(())
}

fn cmd_impls(crate_name: &str, pattern: Option<&str>) -> Result<()> {
    let db = Database::open()?;
    let regex = pattern.map(Regex::new).transpose()?;

    let crate_keys = find_crate_keys_with_reexports(&db, crate_name)?;
    let mut total = 0;

    for crate_key in &crate_keys {
        let impls = db.get_impls(crate_key)?;
        let matches: Vec<&ImplInfo> = impls
            .iter()
            .filter(|i| {
                regex.as_ref().map(|r| {
                    r.is_match(&i.self_type) || i.trait_name.as_ref().map(|t| r.is_match(t)).unwrap_or(false)
                }).unwrap_or(true)
            })
            .collect();

        if !matches.is_empty() {
            println!("── {} ({} impls) ──\n", crate_key, matches.len());
            for i in &matches {
                let impl_desc = match &i.trait_name {
                    Some(trait_name) => format!("impl {} for {}", trait_name, i.self_type),
                    None => format!("impl {}", i.self_type),
                };
                println!("[{}] {}", i.id, truncate_str(&impl_desc, 70));
                println!("  {}:{}", i.file, i.line);
                println!();
            }
            total += matches.len();
        }
    }

    print_summary("impls", total, pattern, crate_keys.len());
    Ok(())
}

fn cmd_show(id: &str) -> Result<()> {
    let db = Database::open()?;

    // Try to find the item in each table
    if let Some((crate_key, func)) = db.get_function_by_id(id)? {
        return show_function(&db, &crate_key, &func);
    }
    if let Some((crate_key, s)) = db.get_struct_by_id(id)? {
        return show_struct(&db, &crate_key, &s);
    }
    if let Some((crate_key, e)) = db.get_enum_by_id(id)? {
        return show_enum(&db, &crate_key, &e);
    }
    if let Some((crate_key, t)) = db.get_trait_by_id(id)? {
        return show_trait(&db, &crate_key, &t);
    }
    if let Some((crate_key, m)) = db.get_macro_by_id(id)? {
        return show_macro(&db, &crate_key, &m);
    }
    if let Some((crate_key, t)) = db.get_type_alias_by_id(id)? {
        return show_type_alias(&db, &crate_key, &t);
    }
    if let Some((crate_key, c)) = db.get_constant_by_id(id)? {
        return show_constant(&db, &crate_key, &c);
    }
    if let Some((crate_key, i)) = db.get_impl_by_id(id)? {
        return show_impl(&db, &crate_key, &i);
    }

    anyhow::bail!("Item with ID '{}' not found", id)
}

fn show_function(db: &Database, crate_key: &str, func: &storage::FunctionInfo) -> Result<()> {
    let crate_path = db.get_crate_path(crate_key)?.unwrap();

    println!("Function: {}", func.name);
    println!("Crate:    {}", crate_key);
    println!("File:     {}", func.file);
    println!("Line:     {}-{}", func.line, func.end_line.map(|l| l.to_string()).unwrap_or("?".to_string()));
    println!("ID:       {}", func.id);
    println!("\nSignature:");
    println!("  {}", func.signature);

    if let Some(docs) = &func.docs {
        println!("\nDocumentation:");
        for line in docs.lines() {
            println!("  /// {}", line);
        }
    }

    show_source(&crate_path, &func.file, func.line, func.end_line)?;
    Ok(())
}

fn show_struct(db: &Database, crate_key: &str, s: &storage::StructInfo) -> Result<()> {
    let crate_path = db.get_crate_path(crate_key)?.unwrap();

    println!("Struct: {}", s.name);
    println!("Crate:  {}", crate_key);
    println!("File:   {}", s.file);
    println!("Line:   {}-{}", s.line, s.end_line.map(|l| l.to_string()).unwrap_or("?".to_string()));
    println!("ID:     {}", s.id);
    println!("Vis:    {}", s.visibility);

    if !s.fields.is_empty() {
        println!("\nFields:");
        for field in &s.fields {
            println!("  {} {}: {}", field.visibility, field.name, field.type_str);
        }
    }

    if let Some(docs) = &s.docs {
        println!("\nDocumentation:");
        for line in docs.lines() {
            println!("  /// {}", line);
        }
    }

    show_source(&crate_path, &s.file, s.line, s.end_line)?;
    Ok(())
}

fn show_enum(db: &Database, crate_key: &str, e: &storage::EnumInfo) -> Result<()> {
    let crate_path = db.get_crate_path(crate_key)?.unwrap();

    println!("Enum:   {}", e.name);
    println!("Crate:  {}", crate_key);
    println!("File:   {}", e.file);
    println!("Line:   {}-{}", e.line, e.end_line.map(|l| l.to_string()).unwrap_or("?".to_string()));
    println!("ID:     {}", e.id);
    println!("Vis:    {}", e.visibility);

    if !e.variants.is_empty() {
        println!("\nVariants:");
        for variant in &e.variants {
            let fields_str = variant.fields.as_ref().map(|f| format!("({})", f)).unwrap_or_default();
            println!("  {}{} [{}]", variant.name, fields_str, variant.kind);
        }
    }

    if let Some(docs) = &e.docs {
        println!("\nDocumentation:");
        for line in docs.lines() {
            println!("  /// {}", line);
        }
    }

    show_source(&crate_path, &e.file, e.line, e.end_line)?;
    Ok(())
}

fn show_trait(db: &Database, crate_key: &str, t: &storage::TraitInfo) -> Result<()> {
    let crate_path = db.get_crate_path(crate_key)?.unwrap();

    println!("Trait:  {}", t.name);
    println!("Crate:  {}", crate_key);
    println!("File:   {}", t.file);
    println!("Line:   {}-{}", t.line, t.end_line.map(|l| l.to_string()).unwrap_or("?".to_string()));
    println!("ID:     {}", t.id);
    println!("Vis:    {}", t.visibility);

    if let Some(docs) = &t.docs {
        println!("\nDocumentation:");
        for line in docs.lines() {
            println!("  /// {}", line);
        }
    }

    show_source(&crate_path, &t.file, t.line, t.end_line)?;
    Ok(())
}

fn show_macro(db: &Database, crate_key: &str, m: &storage::MacroInfo) -> Result<()> {
    let crate_path = db.get_crate_path(crate_key)?.unwrap();

    println!("Macro:  {}!", m.name);
    println!("Crate:  {}", crate_key);
    println!("File:   {}", m.file);
    println!("Line:   {}", m.line);
    println!("ID:     {}", m.id);
    println!("Kind:   {}", m.kind);

    if let Some(docs) = &m.docs {
        println!("\nDocumentation:");
        for line in docs.lines() {
            println!("  /// {}", line);
        }
    }

    // Macros often have no end_line, show more context
    let end = m.end_line.or(Some(m.line + 30));
    show_source(&crate_path, &m.file, m.line, end)?;
    Ok(())
}

fn show_type_alias(db: &Database, crate_key: &str, t: &storage::TypeAliasInfo) -> Result<()> {
    let crate_path = db.get_crate_path(crate_key)?.unwrap();

    println!("Type:   {}", t.name);
    println!("Crate:  {}", crate_key);
    println!("File:   {}", t.file);
    println!("Line:   {}", t.line);
    println!("ID:     {}", t.id);
    println!("Vis:    {}", t.visibility);
    println!("\nDefinition:");
    println!("  type {} = {}", t.name, t.type_str);

    if let Some(docs) = &t.docs {
        println!("\nDocumentation:");
        for line in docs.lines() {
            println!("  /// {}", line);
        }
    }

    show_source(&crate_path, &t.file, t.line, Some(t.line + 5))?;
    Ok(())
}

fn show_constant(db: &Database, crate_key: &str, c: &storage::ConstantInfo) -> Result<()> {
    let crate_path = db.get_crate_path(crate_key)?.unwrap();

    println!("{}: {}", c.kind.to_uppercase(), c.name);
    println!("Crate:  {}", crate_key);
    println!("File:   {}", c.file);
    println!("Line:   {}", c.line);
    println!("ID:     {}", c.id);
    println!("Vis:    {}", c.visibility);
    println!("Type:   {}", c.type_str);

    if let Some(docs) = &c.docs {
        println!("\nDocumentation:");
        for line in docs.lines() {
            println!("  /// {}", line);
        }
    }

    show_source(&crate_path, &c.file, c.line, Some(c.line + 10))?;
    Ok(())
}

fn show_impl(db: &Database, crate_key: &str, i: &storage::ImplInfo) -> Result<()> {
    let crate_path = db.get_crate_path(crate_key)?.unwrap();

    let impl_desc = match &i.trait_name {
        Some(trait_name) => format!("impl {} for {}", trait_name, i.self_type),
        None => format!("impl {}", i.self_type),
    };

    println!("Impl:   {}", impl_desc);
    println!("Crate:  {}", crate_key);
    println!("File:   {}", i.file);
    println!("Line:   {}-{}", i.line, i.end_line.map(|l| l.to_string()).unwrap_or("?".to_string()));
    println!("ID:     {}", i.id);

    show_source(&crate_path, &i.file, i.line, i.end_line)?;
    Ok(())
}

fn show_source(crate_path: &std::path::Path, file: &str, start_line: usize, end_line: Option<usize>) -> Result<()> {
    let source_path = crate_path.join(file);
    if source_path.exists() {
        if let Ok(content) = std::fs::read_to_string(&source_path) {
            let lines: Vec<&str> = content.lines().collect();
            let start = start_line.saturating_sub(1);
            let end = end_line.unwrap_or(start_line + 20).min(lines.len());

            println!("\nSource:");
            for (i, line) in lines[start..end].iter().enumerate() {
                let line_num = start + i + 1;
                println!("{:4} | {}", line_num, line);
            }
        }
    }
    Ok(())
}

fn cmd_latest(crate_name: &str) -> Result<()> {
    let fetcher = Fetcher::new()?;
    let version = fetcher.get_latest_version(crate_name)?;
    println!("{}", version);
    Ok(())
}

fn cmd_readme(crate_name: &str) -> Result<()> {
    let db = Database::open()?;
    let crate_key = find_crate_key(&db, crate_name)?;
    let crate_path = db.get_crate_path(&crate_key)?.unwrap();

    // Look for README files in order of preference
    let readme_names = [
        "README.md",
        "README.markdown",
        "README.txt",
        "README",
        "readme.md",
        "readme.markdown",
        "readme.txt",
        "readme",
    ];

    for name in &readme_names {
        let readme_path = crate_path.join(name);
        if readme_path.exists() {
            let content = std::fs::read_to_string(&readme_path)?;
            println!("── {} ({}) ──\n", crate_key, name);
            println!("{}", content);
            return Ok(());
        }
    }

    anyhow::bail!("No README found in {}", crate_key)
}

fn cmd_read(crate_name: &str, file_path: &str, start: Option<usize>, end: Option<usize>) -> Result<()> {
    let db = Database::open()?;
    let crate_key = find_crate_key(&db, crate_name)?;
    let crate_path = db.get_crate_path(&crate_key)?.unwrap();

    // Security: prevent path traversal attacks
    let file_path = std::path::Path::new(file_path);
    for component in file_path.components() {
        match component {
            std::path::Component::ParentDir => {
                anyhow::bail!("Invalid path: '..' is not allowed");
            }
            std::path::Component::Prefix(_) | std::path::Component::RootDir => {
                anyhow::bail!("Invalid path: absolute paths are not allowed");
            }
            _ => {}
        }
    }

    let full_path = crate_path.join(file_path);

    // Double-check: ensure resolved path is within crate directory
    let canonical_crate = crate_path.canonicalize().unwrap_or_else(|_| crate_path.clone());
    if let Ok(canonical_file) = full_path.canonicalize() {
        if !canonical_file.starts_with(&canonical_crate) {
            anyhow::bail!("Invalid path: path escapes crate directory");
        }
    }

    if !full_path.exists() {
        // List available files if the requested one doesn't exist
        println!("File '{}' not found in {}", file_path.display(), crate_key);
        println!("\nAvailable files:");
        for entry in walkdir::WalkDir::new(&crate_path)
            .into_iter()
            .filter_map(|e| e.ok())
            .filter(|e| e.file_type().is_file())
            .take(20)
        {
            if let Ok(rel) = entry.path().strip_prefix(&crate_path) {
                println!("  {}", rel.display());
            }
        }
        anyhow::bail!("File not found");
    }

    let content = std::fs::read_to_string(&full_path)?;
    let lines: Vec<&str> = content.lines().collect();
    let total_lines = lines.len();

    let start_line = start.unwrap_or(1).max(1);
    let end_line = end.unwrap_or(total_lines).min(total_lines);

    if start_line > total_lines {
        anyhow::bail!("Start line {} exceeds file length ({})", start_line, total_lines);
    }

    println!("── {}:{} ({} total lines) ──\n", crate_key, file_path.display(), total_lines);

    for (i, line) in lines.iter().enumerate() {
        let line_num = i + 1;
        if line_num >= start_line && line_num <= end_line {
            println!("{:4} | {}", line_num, line);
        }
    }

    if end_line < total_lines {
        println!("\n... {} more lines", total_lines - end_line);
    }

    Ok(())
}

fn find_crate_key(db: &Database, name: &str) -> Result<String> {
    // Check if user specified a version (e.g., "anyhow-1.0.100")
    let user_specified_version = name.contains('-') && name.split('-').next_back()
        .map(|s| s.chars().next().map(|c| c.is_ascii_digit()).unwrap_or(false))
        .unwrap_or(false);

    match db.find_crate_key(name)? {
        Some(key) => {
            // If user specified an exact version, use it
            if user_specified_version {
                return Ok(key);
            }

            // Check if there's a newer version available
            let fetcher = Fetcher::new()?;
            let crate_name = extract_crate_name(&key);

            match fetcher.get_latest_version(&crate_name) {
                Ok(latest_version) => {
                    let latest_key = format!("{}-{}", crate_name, latest_version);
                    if latest_key != key {
                        // Newer version available
                        if db.find_crate_key(&latest_key)?.is_none() {
                            println!("Newer version available: {} -> {}. Fetching...", key, latest_key);
                            fetch_single_crate(db, &crate_name, Some(&latest_version))?;
                        }
                        Ok(latest_key)
                    } else {
                        Ok(key)
                    }
                }
                Err(_) => Ok(key), // Can't check, use existing
            }
        }
        None => {
            // Auto-fetch the crate if not found
            println!("Crate '{}' not indexed. Fetching...", name);
            fetch_single_crate(db, name, None)?;

            // Try again after fetching
            db.find_crate_key(name)?
                .ok_or_else(|| anyhow::anyhow!("Failed to fetch crate '{}'", name))
        }
    }
}

fn extract_crate_name(key: &str) -> String {
    // Key format: "crate-name-1.2.3"
    // We need to extract "crate-name" (handle crates with hyphens in names)
    let parts: Vec<&str> = key.rsplitn(2, '-').collect();
    if parts.len() == 2 {
        // Check if the last part looks like a version
        let potential_version = parts[0];
        if potential_version.chars().next().map(|c| c.is_ascii_digit()).unwrap_or(false) {
            return parts[1].to_string();
        }
    }
    key.to_string()
}

fn fetch_single_crate(db: &Database, name: &str, version: Option<&str>) -> Result<()> {
    let fetcher = Fetcher::new()?;
    let mut fetched: HashSet<String> = db.list_crate_keys()?.into_iter().collect();
    let mut queued: HashSet<String> = HashSet::new(); // Track crates already queued
    let mut to_fetch: Vec<(String, Option<String>)> = vec![(name.to_string(), version.map(String::from))];
    queued.insert(name.to_string());

    while !to_fetch.is_empty() {
        // Take current batch
        let batch: Vec<_> = std::mem::take(&mut to_fetch);

        // Resolve versions in parallel
        if batch.len() > 1 {
            println!("Resolving {} crate(s)...", batch.len());
        }
        let resolved: Vec<(String, String)> = batch
            .par_iter()
            .filter_map(|(crate_name, ver)| {
                let version = match ver {
                    Some(v) => v.clone(),
                    None => {
                        match fetcher.get_latest_version(crate_name) {
                            Ok(v) => v,
                            Err(e) => {
                                eprintln!("Warning: Could not fetch {}: {}", crate_name, e);
                                return None;
                            }
                        }
                    }
                };
                let key = format!("{}-{}", crate_name, version);
                if fetched.contains(&key) {
                    None
                } else {
                    Some((crate_name.clone(), version))
                }
            })
            .collect();

        if resolved.is_empty() {
            continue;
        }

        // Deduplicate resolved crates (same crate could be queued from multiple sources)
        let resolved: Vec<_> = resolved
            .into_iter()
            .collect::<HashSet<_>>()
            .into_iter()
            .collect();

        // Download and index in parallel
        if resolved.len() > 1 {
            println!("Downloading and indexing {} crate(s) in parallel...", resolved.len());
        }
        let results: Vec<_> = resolved
            .par_iter()
            .filter_map(|(crate_name, version)| {
                let key = format!("{}-{}", crate_name, version);

                let crate_path = match fetcher.fetch_crate(crate_name, version) {
                    Ok(p) => p,
                    Err(e) => {
                        eprintln!("Warning: Could not download {}: {}", key, e);
                        return None;
                    }
                };

                println!("Indexing {}...", key);
                match index_crate(&crate_path, &key) {
                    Ok(result) => {
                        println!("  {} fns, {} structs, {} enums, {} traits, {} macros, {} types, {} consts, {} impls",
                            result.items.functions.len(),
                            result.items.structs.len(),
                            result.items.enums.len(),
                            result.items.traits.len(),
                            result.items.macros.len(),
                            result.items.type_aliases.len(),
                            result.items.constants.len(),
                            result.items.impls.len());
                        Some((key, crate_path, result))
                    }
                    Err(e) => {
                        eprintln!("Warning: Failed to index {}: {}", key, e);
                        None
                    }
                }
            })
            .collect();

        // Store results and collect re-exports (sequential for DB writes)
        for (key, crate_path, result) in results {
            if !result.reexported_crates.is_empty() {
                println!("  {} re-exports: {:?}", key, result.reexported_crates);

                for reexport in &result.reexported_crates {
                    let already_have = fetched.iter().any(|k| k.starts_with(&format!("{}-", reexport)));
                    if !already_have && !queued.contains(reexport) {
                        queued.insert(reexport.clone());
                        to_fetch.push((reexport.clone(), None));
                    }
                }
            }

            db.add_crate(&key, &crate_path, &result.items, &result.reexported_crates)?;
            fetched.insert(key);
        }
    }

    Ok(())
}

fn find_crate_keys_with_reexports(db: &Database, name: &str) -> Result<Vec<String>> {
    let root_key = find_crate_key(db, name)?;
    let mut keys = vec![root_key.clone()];
    let mut seen: HashSet<String> = HashSet::new();
    seen.insert(root_key.clone());

    // Recursively collect all reexported crates
    let mut to_process = vec![root_key];
    while let Some(key) = to_process.pop() {
        for reexport in db.get_reexports(&key)? {
            // Find the actual indexed key for this reexport
            if let Some(reexport_key) = db.find_crate_key(&reexport)? {
                if !seen.contains(&reexport_key) {
                    seen.insert(reexport_key.clone());
                    keys.push(reexport_key.clone());
                    to_process.push(reexport_key);
                }
            }
        }
    }

    Ok(keys)
}

async fn cmd_semantic_search(crate_name: &str, query: &str, limit: usize) -> Result<()> {
    // Run blocking operations (database + potential fetcher) in spawn_blocking
    // Get all matching crate keys (handles multiple versions) and their re-exports
    let crate_name_owned = crate_name.to_string();
    let (main_crate_keys, all_crate_keys) = tokio::task::spawn_blocking(move || {
        let db = Database::open()?;

        // Find all versions matching the name
        let main_keys = db.find_all_crate_keys(&crate_name_owned)?;
        if main_keys.is_empty() {
            return Ok::<_, anyhow::Error>((vec![], vec![]));
        }

        // Collect all keys including re-exports from all versions
        let mut all_keys = Vec::new();
        let mut seen = std::collections::HashSet::new();

        for main_key in &main_keys {
            let keys_with_reexports = find_crate_keys_with_reexports(&db, main_key)?;
            for key in keys_with_reexports {
                if !seen.contains(&key) {
                    seen.insert(key.clone());
                    all_keys.push(key);
                }
            }
        }

        Ok((main_keys, all_keys))
    }).await??;

    if all_crate_keys.is_empty() {
        println!("No crates found for '{}'", crate_name);
        return Ok(());
    }

    // Generate embeddings for all crates that need them
    for key in &all_crate_keys {
        let key_clone = key.clone();
        let has_embeddings = tokio::task::spawn_blocking(move || {
            let db = Database::open()?;
            db.has_embeddings(&key_clone)
        }).await??;

        if !has_embeddings {
            println!("Generating embeddings for {}...", key);
            generate_embeddings_async(key).await?;
        }
    }

    // Initialize embedding manager for query
    println!("Initializing embedding model...");
    let embedder = EmbeddingManager::new()?;

    // Perform semantic search
    println!("Searching for: {}\n", query);

    // Get stored embeddings from all crates
    let keys_for_search = all_crate_keys.clone();
    let stored_embeddings = tokio::task::spawn_blocking(move || {
        let db = Database::open()?;
        let mut all_embeddings = Vec::new();
        for key in &keys_for_search {
            all_embeddings.extend(db.get_all_embeddings(key)?);
        }
        Ok::<_, anyhow::Error>(all_embeddings)
    }).await??;

    // Embed query and compute similarities
    let query_embedding = embedder.embed_query(query).await?;

    use crate::embeddings::{bytes_to_embedding, cosine_similarity};
    use rayon::prelude::*;
    let mut results: Vec<crate::search::SemanticSearchResult> = stored_embeddings
        .par_iter()
        .map(|info| {
            let embedding = bytes_to_embedding(&info.embedding);
            let similarity = cosine_similarity(&query_embedding, &embedding);
            crate::search::SemanticSearchResult {
                item_id: info.id.clone(),
                item_type: info.item_type.clone(),
                similarity,
                text_content: info.text_content.clone(),
                crate_key: info.crate_key.clone(),
            }
        })
        .collect();

    results.sort_by(|a, b| b.similarity.partial_cmp(&a.similarity).unwrap_or(std::cmp::Ordering::Equal));
    results.truncate(limit);

    // Build header showing what we searched
    let header = if main_crate_keys.len() > 1 {
        format!("── {} versions + {} total crates ({} results) ──\n",
            main_crate_keys.len(), all_crate_keys.len(), results.len())
    } else if all_crate_keys.len() > 1 {
        format!("── {} + {} re-exports ({} results) ──\n",
            main_crate_keys[0], all_crate_keys.len() - 1, results.len())
    } else {
        format!("── {} ({} results) ──\n", main_crate_keys[0], results.len())
    };

    if results.is_empty() {
        println!("No results found.");
    } else {
        println!("{}", header);
        for result in &results {
            println!("[{}] {} in {} (score: {:.3})",
                result.item_id, result.item_type, result.crate_key, result.similarity);
            // Show truncated text content
            let text = result.text_content.lines().next().unwrap_or("");
            let truncated = truncate_str(text, 80);
            println!("  {}", truncated);
            println!();
        }
    }

    Ok(())
}

async fn cmd_embed(crate_name: &str) -> Result<()> {
    // Run blocking operations in spawn_blocking
    let crate_name = crate_name.to_string();
    let (crate_key, has_embeddings) = tokio::task::spawn_blocking(move || {
        let db = Database::open()?;
        let crate_key = find_crate_key(&db, &crate_name)?;
        let has_embeddings = db.has_embeddings(&crate_key)?;
        Ok::<_, anyhow::Error>((crate_key, has_embeddings))
    }).await??;

    if has_embeddings {
        println!("Embeddings already exist for {}. Re-generating...", crate_key);
    }

    generate_embeddings_async(&crate_key).await?;
    println!("Done!");

    Ok(())
}

async fn generate_embeddings_async(crate_key: &str) -> Result<()> {
    println!("Initializing embedding model...");
    let embedder = EmbeddingManager::new()?;

    // Phase 1: Collect all items from database (blocking)
    let crate_key_owned = crate_key.to_string();
    let (items_to_embed, crate_id) = tokio::task::spawn_blocking(move || {
        let db = Database::open()?;
        let mut items: Vec<(String, String, String)> = Vec::new(); // (id, type, text)

        // Functions
        for func in db.get_functions(&crate_key_owned)? {
            let text = format_function_for_embedding(&func);
            items.push((func.id, "function".to_string(), text));
        }

        // Structs
        for s in db.get_structs(&crate_key_owned)? {
            let text = format_struct_for_embedding(&s);
            items.push((s.id, "struct".to_string(), text));
        }

        // Enums
        for e in db.get_enums(&crate_key_owned)? {
            let text = format_enum_for_embedding(&e);
            items.push((e.id, "enum".to_string(), text));
        }

        // Traits
        for t in db.get_traits(&crate_key_owned)? {
            let text = format_trait_for_embedding(&t);
            items.push((t.id, "trait".to_string(), text));
        }

        // Macros
        for m in db.get_macros(&crate_key_owned)? {
            let text = format_macro_for_embedding(&m);
            items.push((m.id, "macro".to_string(), text));
        }

        // Type aliases
        for t in db.get_type_aliases(&crate_key_owned)? {
            let text = format_type_alias_for_embedding(&t);
            items.push((t.id, "type_alias".to_string(), text));
        }

        // Constants
        for c in db.get_constants(&crate_key_owned)? {
            let text = format_constant_for_embedding(&c);
            items.push((c.id, "constant".to_string(), text));
        }

        let crate_id = db.get_crate_id(&crate_key_owned)?
            .ok_or_else(|| anyhow::anyhow!("Crate not found"))?;

        Ok::<_, anyhow::Error>((items, crate_id))
    }).await??;

    if items_to_embed.is_empty() {
        println!("No items to embed.");
        return Ok(());
    }

    println!("Embedding {} items...", items_to_embed.len());

    // Phase 2: Generate embeddings (async)
    let texts: Vec<String> = items_to_embed.iter().map(|(_, _, t)| t.clone()).collect();
    let embeddings = embedder.embed_texts(&texts).await?;

    // Prepare for storage
    let embeddings_to_store: Vec<(String, String, Vec<u8>, String)> = items_to_embed
        .into_iter()
        .zip(embeddings)
        .map(|((id, item_type, text), emb)| {
            let bytes = embedding_to_bytes(&emb);
            (id, item_type, bytes, text)
        })
        .collect();

    // Phase 3: Save to database (blocking)
    let count = embeddings_to_store.len();
    tokio::task::spawn_blocking(move || {
        let db = Database::open()?;
        db.save_embeddings(crate_id, &embeddings_to_store)?;
        Ok::<_, anyhow::Error>(())
    }).await??;

    println!("Stored {} embeddings.", count);

    Ok(())
}

fn format_function_for_embedding(func: &storage::FunctionInfo) -> String {
    let mut text = func.signature.clone();
    if let Some(docs) = &func.docs {
        text.push_str(". ");
        text.push_str(docs);
    }
    text
}

fn format_struct_for_embedding(s: &storage::StructInfo) -> String {
    let mut text = format!("struct {}", s.name);
    if !s.fields.is_empty() {
        let field_names: Vec<&str> = s.fields.iter().map(|f| f.name.as_str()).collect();
        text.push_str(" with fields: ");
        text.push_str(&field_names.join(", "));
    }
    if let Some(docs) = &s.docs {
        text.push_str(". ");
        text.push_str(docs);
    }
    text
}

fn format_enum_for_embedding(e: &storage::EnumInfo) -> String {
    let mut text = format!("enum {}", e.name);
    if !e.variants.is_empty() {
        let variant_names: Vec<&str> = e.variants.iter().map(|v| v.name.as_str()).collect();
        text.push_str(" with variants: ");
        text.push_str(&variant_names.join(", "));
    }
    if let Some(docs) = &e.docs {
        text.push_str(". ");
        text.push_str(docs);
    }
    text
}

fn format_trait_for_embedding(t: &storage::TraitInfo) -> String {
    let mut text = format!("trait {}", t.name);
    if let Some(docs) = &t.docs {
        text.push_str(". ");
        text.push_str(docs);
    }
    text
}

fn format_macro_for_embedding(m: &storage::MacroInfo) -> String {
    let mut text = format!("macro {}!", m.name);
    if let Some(docs) = &m.docs {
        text.push_str(". ");
        text.push_str(docs);
    }
    text
}

fn format_type_alias_for_embedding(t: &storage::TypeAliasInfo) -> String {
    let mut text = format!("type {} = {}", t.name, t.type_str);
    if let Some(docs) = &t.docs {
        text.push_str(". ");
        text.push_str(docs);
    }
    text
}

fn format_constant_for_embedding(c: &storage::ConstantInfo) -> String {
    let mut text = format!("{} {}: {}", c.kind, c.name, c.type_str);
    if let Some(docs) = &c.docs {
        text.push_str(". ");
        text.push_str(docs);
    }
    text
}

fn truncate_str(s: &str, max_len: usize) -> String {
    if s.len() > max_len {
        format!("{}...", &s[..max_len.saturating_sub(3)])
    } else {
        s.to_string()
    }
}

fn print_summary(item_type: &str, total: usize, pattern: Option<&str>, crate_count: usize) {
    if total == 0 {
        if let Some(p) = pattern {
            println!("No {} matching '{}'", item_type, p);
        } else {
            println!("No {} found", item_type);
        }
    } else {
        println!("Total: {} {} across {} crate(s)", total, item_type, crate_count);
    }
}
