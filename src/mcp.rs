use anyhow::Result;
use rmcp::{
    handler::server::router::tool::ToolRouter,
    handler::server::tool::Parameters,
    model::{ErrorData as McpError, *},
    tool, tool_handler, tool_router, ServerHandler,
    transport::stdio,
    ServiceExt,
};
use serde::Deserialize;
use std::borrow::Cow;
use std::collections::HashSet;
use std::future::Future; // Required by #[tool] macro

use crate::embeddings::{embedding_to_bytes, EmbeddingManager};
use crate::fetcher::Fetcher;
use crate::indexer::index_crate;
use crate::search::{build_regex, search_functions, search_regex};
use crate::storage::Database;

#[derive(Debug, Clone)]
pub struct CrateIndexerServer {
    tool_router: ToolRouter<CrateIndexerServer>,
}

// Request types with JSON schema
#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct FetchCrateRequest {
    #[schemars(description = "Name of the crate to fetch")]
    pub crate_name: String,
    #[schemars(description = "Specific version (optional, defaults to latest)")]
    pub version: Option<String>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct SearchCrateRequest {
    #[schemars(description = "Name of the crate to search")]
    pub crate_name: String,
    #[schemars(description = "Regex pattern to search for")]
    pub pattern: String,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct ListItemsRequest {
    #[schemars(description = "Name of the crate")]
    pub crate_name: String,
    #[schemars(description = "Optional regex pattern to filter results")]
    pub pattern: Option<String>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct ShowItemRequest {
    #[schemars(description = "Item ID (8-character hex)")]
    pub id: String,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct ReadFileRequest {
    #[schemars(description = "Name of the crate")]
    pub crate_name: String,
    #[schemars(description = "Path to file within the crate (e.g., 'src/lib.rs')")]
    pub file_path: String,
    #[schemars(description = "Start line number (1-indexed)")]
    pub start_line: Option<usize>,
    #[schemars(description = "End line number (1-indexed)")]
    pub end_line: Option<usize>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct ReadmeRequest {
    #[schemars(description = "Name of the crate")]
    pub crate_name: String,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct SemanticSearchRequest {
    #[schemars(description = "Name of the crate to search")]
    pub crate_name: String,
    #[schemars(description = "Natural language search query")]
    pub query: String,
    #[schemars(description = "Maximum number of results (default 10)")]
    pub limit: Option<usize>,
}

fn make_error(msg: String) -> McpError {
    McpError {
        code: ErrorCode::INTERNAL_ERROR,
        message: Cow::from(msg),
        data: None,
    }
}

#[tool_router]
impl CrateIndexerServer {
    pub fn new() -> Self {
        Self {
            tool_router: Self::tool_router(),
        }
    }

    #[tool(description = "Fetch and index a crate from crates.io. Automatically fetches re-exported dependencies.")]
    async fn fetch_crate(
        &self,
        Parameters(req): Parameters<FetchCrateRequest>,
    ) -> Result<CallToolResult, McpError> {
        let result = tokio::task::spawn_blocking(move || {
            do_fetch_crate(&req.crate_name, req.version.as_deref())
        })
        .await
        .map_err(|e| make_error(format!("Task error: {}", e)))?
        .map_err(|e| make_error(format!("{}", e)))?;

        Ok(CallToolResult::success(vec![Content::text(result)]))
    }

    #[tool(description = "Search a crate's source code with a regex pattern")]
    async fn search_crate(
        &self,
        Parameters(req): Parameters<SearchCrateRequest>,
    ) -> Result<CallToolResult, McpError> {
        let result = tokio::task::spawn_blocking(move || {
            do_search_crate(&req.crate_name, &req.pattern)
        })
        .await
        .map_err(|e| make_error(format!("Task error: {}", e)))?
        .map_err(|e| make_error(format!("{}", e)))?;

        Ok(CallToolResult::success(vec![Content::text(result)]))
    }

    #[tool(description = "List or search function definitions in a crate")]
    async fn list_functions(
        &self,
        Parameters(req): Parameters<ListItemsRequest>,
    ) -> Result<CallToolResult, McpError> {
        let result = tokio::task::spawn_blocking(move || {
            do_list_functions(&req.crate_name, req.pattern.as_deref())
        })
        .await
        .map_err(|e| make_error(format!("Task error: {}", e)))?
        .map_err(|e| make_error(format!("{}", e)))?;

        Ok(CallToolResult::success(vec![Content::text(result)]))
    }

    #[tool(description = "List or search struct definitions in a crate")]
    async fn list_structs(
        &self,
        Parameters(req): Parameters<ListItemsRequest>,
    ) -> Result<CallToolResult, McpError> {
        let result = tokio::task::spawn_blocking(move || {
            do_list_structs(&req.crate_name, req.pattern.as_deref())
        })
        .await
        .map_err(|e| make_error(format!("Task error: {}", e)))?
        .map_err(|e| make_error(format!("{}", e)))?;

        Ok(CallToolResult::success(vec![Content::text(result)]))
    }

    #[tool(description = "List or search enum definitions in a crate")]
    async fn list_enums(
        &self,
        Parameters(req): Parameters<ListItemsRequest>,
    ) -> Result<CallToolResult, McpError> {
        let result = tokio::task::spawn_blocking(move || {
            do_list_enums(&req.crate_name, req.pattern.as_deref())
        })
        .await
        .map_err(|e| make_error(format!("Task error: {}", e)))?
        .map_err(|e| make_error(format!("{}", e)))?;

        Ok(CallToolResult::success(vec![Content::text(result)]))
    }

    #[tool(description = "List or search trait definitions in a crate")]
    async fn list_traits(
        &self,
        Parameters(req): Parameters<ListItemsRequest>,
    ) -> Result<CallToolResult, McpError> {
        let result = tokio::task::spawn_blocking(move || {
            do_list_traits(&req.crate_name, req.pattern.as_deref())
        })
        .await
        .map_err(|e| make_error(format!("Task error: {}", e)))?
        .map_err(|e| make_error(format!("{}", e)))?;

        Ok(CallToolResult::success(vec![Content::text(result)]))
    }

    #[tool(description = "List or search impl blocks in a crate")]
    async fn list_impls(
        &self,
        Parameters(req): Parameters<ListItemsRequest>,
    ) -> Result<CallToolResult, McpError> {
        let result = tokio::task::spawn_blocking(move || {
            do_list_impls(&req.crate_name, req.pattern.as_deref())
        })
        .await
        .map_err(|e| make_error(format!("Task error: {}", e)))?
        .map_err(|e| make_error(format!("{}", e)))?;

        Ok(CallToolResult::success(vec![Content::text(result)]))
    }

    #[tool(description = "Show full details of an item by ID, including source code")]
    async fn show_item(
        &self,
        Parameters(req): Parameters<ShowItemRequest>,
    ) -> Result<CallToolResult, McpError> {
        let result = tokio::task::spawn_blocking(move || do_show_item(&req.id))
            .await
            .map_err(|e| make_error(format!("Task error: {}", e)))?
            .map_err(|e| make_error(format!("{}", e)))?;

        Ok(CallToolResult::success(vec![Content::text(result)]))
    }

    #[tool(description = "Read a file from an indexed crate")]
    async fn read_file(
        &self,
        Parameters(req): Parameters<ReadFileRequest>,
    ) -> Result<CallToolResult, McpError> {
        let result = tokio::task::spawn_blocking(move || {
            do_read_file(&req.crate_name, &req.file_path, req.start_line, req.end_line)
        })
        .await
        .map_err(|e| make_error(format!("Task error: {}", e)))?
        .map_err(|e| make_error(format!("{}", e)))?;

        Ok(CallToolResult::success(vec![Content::text(result)]))
    }

    #[tool(description = "Get the README of a crate")]
    async fn read_readme(
        &self,
        Parameters(req): Parameters<ReadmeRequest>,
    ) -> Result<CallToolResult, McpError> {
        let result = tokio::task::spawn_blocking(move || do_read_readme(&req.crate_name))
            .await
            .map_err(|e| make_error(format!("Task error: {}", e)))?
            .map_err(|e| make_error(format!("{}", e)))?;

        Ok(CallToolResult::success(vec![Content::text(result)]))
    }

    #[tool(description = "Semantic search for code in a crate using natural language. Finds functions, structs, enums, traits, etc. based on meaning, not just keywords. Note: This is significantly slower than search_crate (regex) as it requires generating embeddings. Use search_crate for simple keyword/pattern matching.")]
    async fn semantic_search(
        &self,
        Parameters(req): Parameters<SemanticSearchRequest>,
    ) -> Result<CallToolResult, McpError> {
        let crate_name = req.crate_name;
        let query = req.query;
        let limit = req.limit.unwrap_or(10);

        let result = do_semantic_search(&crate_name, &query, limit)
            .await
            .map_err(|e| make_error(format!("{}", e)))?;

        Ok(CallToolResult::success(vec![Content::text(result)]))
    }
}

#[tool_handler]
impl ServerHandler for CrateIndexerServer {
    fn get_info(&self) -> ServerInfo {
        ServerInfo {
            protocol_version: ProtocolVersion::V_2024_11_05,
            capabilities: ServerCapabilities::builder().enable_tools().build(),
            server_info: Implementation::from_build_env(),
            instructions: Some(
                "Crate indexer for searching and exploring Rust crates from crates.io. \
                 Use fetch_crate to download and index a crate, then use search_crate \
                 for regex searches or list_* functions to browse definitions."
                    .to_string(),
            ),
        }
    }
}

pub async fn run_mcp_server() -> Result<()> {
    let service = CrateIndexerServer::new().serve(stdio()).await?;
    service.waiting().await?;
    Ok(())
}

// Implementation functions

fn ensure_crate(db: &Database, name: &str) -> anyhow::Result<String> {
    // For MCP: use what's indexed, only auto-fetch if crate is not found at all.
    // This avoids network calls on every operation.
    match db.find_crate_key(name)? {
        Some(key) => Ok(key),
        None => {
            // Auto-fetch only when crate is not found
            do_fetch_crate(name, None)?;
            db.find_crate_key(name)?
                .ok_or_else(|| anyhow::anyhow!("Failed to fetch crate '{}'", name))
        }
    }
}

fn do_fetch_crate(name: &str, version: Option<&str>) -> anyhow::Result<String> {
    use rayon::prelude::*;

    let db = Database::open()?;
    let fetcher = Fetcher::new()?;
    let mut fetched: HashSet<String> = db.list_crate_keys()?.into_iter().collect();
    let mut queued: HashSet<String> = HashSet::new();
    let mut to_fetch: Vec<(String, Option<String>)> = vec![(name.to_string(), version.map(String::from))];
    queued.insert(name.to_string());

    let mut output = String::new();

    while !to_fetch.is_empty() {
        let batch: Vec<_> = std::mem::take(&mut to_fetch);

        let resolved: Vec<(String, String)> = batch
            .par_iter()
            .filter_map(|(crate_name, ver)| {
                let version = match ver {
                    Some(v) => v.clone(),
                    None => fetcher.get_latest_version(crate_name).ok()?,
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

        let resolved: Vec<_> = resolved.into_iter().collect::<HashSet<_>>().into_iter().collect();

        let results: Vec<_> = resolved
            .par_iter()
            .filter_map(|(crate_name, version)| {
                let key = format!("{}-{}", crate_name, version);
                let crate_path = fetcher.fetch_crate(crate_name, version).ok()?;
                let result = index_crate(&crate_path, &key).ok()?;
                Some((key, crate_path, result))
            })
            .collect();

        for (key, crate_path, result) in results {
            output.push_str(&format!(
                "Indexed {}: {} functions, {} structs, {} enums, {} traits\n",
                key,
                result.items.functions.len(),
                result.items.structs.len(),
                result.items.enums.len(),
                result.items.traits.len()
            ));

            for reexport in &result.reexported_crates {
                let already_have = fetched.iter().any(|k| k.starts_with(&format!("{}-", reexport)));
                if !already_have && !queued.contains(reexport) {
                    queued.insert(reexport.clone());
                    to_fetch.push((reexport.clone(), None));
                }
            }

            db.add_crate(&key, &crate_path, &result.items, &result.reexported_crates)?;
            fetched.insert(key);
        }
    }

    if output.is_empty() {
        Ok(format!("Crate {} is already indexed", name))
    } else {
        Ok(output)
    }
}

fn do_search_crate(crate_name: &str, pattern: &str) -> anyhow::Result<String> {
    let db = Database::open()?;
    let crate_key = ensure_crate(&db, crate_name)?;
    let crate_path = db.get_crate_path(&crate_key)?
        .ok_or_else(|| anyhow::anyhow!("Crate path not found"))?;

    let matches = search_regex(&crate_path, pattern)?;

    let mut output = String::new();
    for m in matches.iter().take(50) {
        output.push_str(&format!("{}:{}: {}\n", m.file, m.line, m.content));
    }

    if matches.len() > 50 {
        output.push_str(&format!("\n... and {} more matches\n", matches.len() - 50));
    }

    output.push_str(&format!("\nTotal: {} matches", matches.len()));
    Ok(output)
}

fn do_list_functions(crate_name: &str, pattern: Option<&str>) -> anyhow::Result<String> {
    let db = Database::open()?;
    let crate_key = ensure_crate(&db, crate_name)?;

    let functions = db.get_functions(&crate_key)?;
    let matches = search_functions(&functions, pattern)?;

    let mut output = String::new();
    for func in matches.iter().take(50) {
        output.push_str(&format!("[{}] {}\n", func.id, func.signature));
        output.push_str(&format!("  {}:{}\n", func.file, func.line));
        if let Some(docs) = &func.docs {
            let first_line = docs.lines().next().unwrap_or("");
            if !first_line.is_empty() {
                output.push_str(&format!("  /// {}\n", truncate(first_line, 80)));
            }
        }
        output.push('\n');
    }

    if matches.len() > 50 {
        output.push_str(&format!("... and {} more functions\n", matches.len() - 50));
    }

    output.push_str(&format!("Total: {} functions", matches.len()));
    Ok(output)
}

fn do_list_structs(crate_name: &str, pattern: Option<&str>) -> anyhow::Result<String> {
    let db = Database::open()?;
    let crate_key = ensure_crate(&db, crate_name)?;

    let structs = db.get_structs(&crate_key)?;
    let regex = pattern.map(|p| build_regex(p)).transpose()?;

    let matches: Vec<_> = structs.iter()
        .filter(|s| regex.as_ref().map(|r| r.is_match(&s.name)).unwrap_or(true))
        .collect();

    let mut output = String::new();
    for s in matches.iter().take(50) {
        output.push_str(&format!("[{}] {} struct {}\n", s.id, s.visibility, s.name));
        output.push_str(&format!("  {}:{}\n", s.file, s.line));
        if !s.fields.is_empty() {
            let field_names: Vec<_> = s.fields.iter().take(5).map(|f| f.name.as_str()).collect();
            output.push_str(&format!("  Fields: {}\n", field_names.join(", ")));
        }
        output.push('\n');
    }

    if matches.len() > 50 {
        output.push_str(&format!("... and {} more structs\n", matches.len() - 50));
    }

    output.push_str(&format!("Total: {} structs", matches.len()));
    Ok(output)
}

fn do_list_enums(crate_name: &str, pattern: Option<&str>) -> anyhow::Result<String> {
    let db = Database::open()?;
    let crate_key = ensure_crate(&db, crate_name)?;

    let enums = db.get_enums(&crate_key)?;
    let regex = pattern.map(|p| build_regex(p)).transpose()?;

    let matches: Vec<_> = enums.iter()
        .filter(|e| regex.as_ref().map(|r| r.is_match(&e.name)).unwrap_or(true))
        .collect();

    let mut output = String::new();
    for e in matches.iter().take(50) {
        output.push_str(&format!("[{}] {} enum {}\n", e.id, e.visibility, e.name));
        output.push_str(&format!("  {}:{}\n", e.file, e.line));
        let variant_names: Vec<_> = e.variants.iter().take(5).map(|v| v.name.as_str()).collect();
        output.push_str(&format!("  Variants: {}\n", variant_names.join(", ")));
        output.push('\n');
    }

    if matches.len() > 50 {
        output.push_str(&format!("... and {} more enums\n", matches.len() - 50));
    }

    output.push_str(&format!("Total: {} enums", matches.len()));
    Ok(output)
}

fn do_list_traits(crate_name: &str, pattern: Option<&str>) -> anyhow::Result<String> {
    let db = Database::open()?;
    let crate_key = ensure_crate(&db, crate_name)?;

    let traits = db.get_traits(&crate_key)?;
    let regex = pattern.map(|p| build_regex(p)).transpose()?;

    let matches: Vec<_> = traits.iter()
        .filter(|t| regex.as_ref().map(|r| r.is_match(&t.name)).unwrap_or(true))
        .collect();

    let mut output = String::new();
    for t in matches.iter().take(50) {
        output.push_str(&format!("[{}] {} trait {}\n", t.id, t.visibility, t.name));
        output.push_str(&format!("  {}:{}\n", t.file, t.line));
        if let Some(docs) = &t.docs {
            let first_line = docs.lines().next().unwrap_or("");
            if !first_line.is_empty() {
                output.push_str(&format!("  /// {}\n", truncate(first_line, 80)));
            }
        }
        output.push('\n');
    }

    if matches.len() > 50 {
        output.push_str(&format!("... and {} more traits\n", matches.len() - 50));
    }

    output.push_str(&format!("Total: {} traits", matches.len()));
    Ok(output)
}

fn do_list_impls(crate_name: &str, pattern: Option<&str>) -> anyhow::Result<String> {
    let db = Database::open()?;
    let crate_key = ensure_crate(&db, crate_name)?;

    let impls = db.get_impls(&crate_key)?;
    let regex = pattern.map(|p| build_regex(p)).transpose()?;

    let matches: Vec<_> = impls.iter()
        .filter(|i| {
            regex.as_ref().map(|r| {
                r.is_match(&i.self_type) || i.trait_name.as_ref().map(|t| r.is_match(t)).unwrap_or(false)
            }).unwrap_or(true)
        })
        .collect();

    let mut output = String::new();
    for i in matches.iter().take(50) {
        let impl_desc = match &i.trait_name {
            Some(trait_name) => format!("impl {} for {}", trait_name, i.self_type),
            None => format!("impl {}", i.self_type),
        };
        output.push_str(&format!("[{}] {}\n", i.id, impl_desc));
        output.push_str(&format!("  {}:{}\n\n", i.file, i.line));
    }

    if matches.len() > 50 {
        output.push_str(&format!("... and {} more impls\n", matches.len() - 50));
    }

    output.push_str(&format!("Total: {} impls", matches.len()));
    Ok(output)
}

fn do_show_item(id: &str) -> anyhow::Result<String> {
    let db = Database::open()?;

    if let Some((crate_key, func)) = db.get_function_by_id(id)? {
        return show_function_detail(&db, &crate_key, &func);
    }
    if let Some((crate_key, s)) = db.get_struct_by_id(id)? {
        return show_struct_detail(&db, &crate_key, &s);
    }
    if let Some((crate_key, e)) = db.get_enum_by_id(id)? {
        return show_enum_detail(&db, &crate_key, &e);
    }
    if let Some((crate_key, t)) = db.get_trait_by_id(id)? {
        return show_trait_detail(&db, &crate_key, &t);
    }
    if let Some((crate_key, i)) = db.get_impl_by_id(id)? {
        return show_impl_detail(&db, &crate_key, &i);
    }

    anyhow::bail!("Item with ID '{}' not found", id)
}

fn show_function_detail(db: &Database, crate_key: &str, func: &crate::storage::FunctionInfo) -> anyhow::Result<String> {
    let mut output = String::new();
    output.push_str(&format!("Function: {}\n", func.name));
    output.push_str(&format!("Crate: {}\n", crate_key));
    output.push_str(&format!("File: {}:{}\n", func.file, func.line));
    output.push_str(&format!("ID: {}\n\n", func.id));
    output.push_str(&format!("Signature:\n  {}\n", func.signature));

    if let Some(docs) = &func.docs {
        output.push_str("\nDocumentation:\n");
        for line in docs.lines() {
            output.push_str(&format!("  /// {}\n", line));
        }
    }

    output.push_str(&format!("\n{}", get_source(db, crate_key, &func.file, func.line, func.end_line)?));
    Ok(output)
}

fn show_struct_detail(db: &Database, crate_key: &str, s: &crate::storage::StructInfo) -> anyhow::Result<String> {
    let mut output = String::new();
    output.push_str(&format!("Struct: {}\n", s.name));
    output.push_str(&format!("Crate: {}\n", crate_key));
    output.push_str(&format!("File: {}:{}\n", s.file, s.line));
    output.push_str(&format!("Visibility: {}\n", s.visibility));
    output.push_str(&format!("ID: {}\n", s.id));

    if !s.fields.is_empty() {
        output.push_str("\nFields:\n");
        for field in &s.fields {
            output.push_str(&format!("  {} {}: {}\n", field.visibility, field.name, field.type_str));
        }
    }

    output.push_str(&format!("\n{}", get_source(db, crate_key, &s.file, s.line, s.end_line)?));
    Ok(output)
}

fn show_enum_detail(db: &Database, crate_key: &str, e: &crate::storage::EnumInfo) -> anyhow::Result<String> {
    let mut output = String::new();
    output.push_str(&format!("Enum: {}\n", e.name));
    output.push_str(&format!("Crate: {}\n", crate_key));
    output.push_str(&format!("File: {}:{}\n", e.file, e.line));
    output.push_str(&format!("Visibility: {}\n", e.visibility));
    output.push_str(&format!("ID: {}\n", e.id));

    if !e.variants.is_empty() {
        output.push_str("\nVariants:\n");
        for v in &e.variants {
            let fields = v.fields.as_ref().map(|f| format!("({})", f)).unwrap_or_default();
            output.push_str(&format!("  {}{}\n", v.name, fields));
        }
    }

    output.push_str(&format!("\n{}", get_source(db, crate_key, &e.file, e.line, e.end_line)?));
    Ok(output)
}

fn show_trait_detail(db: &Database, crate_key: &str, t: &crate::storage::TraitInfo) -> anyhow::Result<String> {
    let mut output = String::new();
    output.push_str(&format!("Trait: {}\n", t.name));
    output.push_str(&format!("Crate: {}\n", crate_key));
    output.push_str(&format!("File: {}:{}\n", t.file, t.line));
    output.push_str(&format!("Visibility: {}\n", t.visibility));
    output.push_str(&format!("ID: {}\n", t.id));

    if let Some(docs) = &t.docs {
        output.push_str("\nDocumentation:\n");
        for line in docs.lines() {
            output.push_str(&format!("  /// {}\n", line));
        }
    }

    output.push_str(&format!("\n{}", get_source(db, crate_key, &t.file, t.line, t.end_line)?));
    Ok(output)
}

fn show_impl_detail(db: &Database, crate_key: &str, i: &crate::storage::ImplInfo) -> anyhow::Result<String> {
    let mut output = String::new();
    let impl_desc = match &i.trait_name {
        Some(trait_name) => format!("impl {} for {}", trait_name, i.self_type),
        None => format!("impl {}", i.self_type),
    };
    output.push_str(&format!("Impl: {}\n", impl_desc));
    output.push_str(&format!("Crate: {}\n", crate_key));
    output.push_str(&format!("File: {}:{}\n", i.file, i.line));
    output.push_str(&format!("ID: {}\n", i.id));

    output.push_str(&format!("\n{}", get_source(db, crate_key, &i.file, i.line, i.end_line)?));
    Ok(output)
}

fn get_source(db: &Database, crate_key: &str, file: &str, start: usize, end: Option<usize>) -> anyhow::Result<String> {
    let crate_path = db.get_crate_path(crate_key)?
        .ok_or_else(|| anyhow::anyhow!("Crate path not found"))?;

    let source_path = crate_path.join(file);
    if !source_path.exists() {
        anyhow::bail!("Source file '{}' not found in {}", file, crate_key);
    }

    let content = std::fs::read_to_string(&source_path)?;
    let lines: Vec<&str> = content.lines().collect();
    let start_idx = start.saturating_sub(1);
    let end_idx = end.unwrap_or(start + 30).min(lines.len());

    let mut output = String::from("Source:\n");
    for (i, line) in lines[start_idx..end_idx].iter().enumerate() {
        output.push_str(&format!("{:4} | {}\n", start_idx + i + 1, line));
    }

    Ok(output)
}

const MAX_DEFAULT_LINES: usize = 500;

fn do_read_file(crate_name: &str, file_path: &str, start: Option<usize>, end: Option<usize>) -> anyhow::Result<String> {
    let db = Database::open()?;
    let crate_key = ensure_crate(&db, crate_name)?;
    let crate_path = db.get_crate_path(&crate_key)?
        .ok_or_else(|| anyhow::anyhow!("Crate path not found"))?;

    // Security check
    let file_path_obj = std::path::Path::new(file_path);
    for component in file_path_obj.components() {
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
    if !full_path.exists() {
        anyhow::bail!("File '{}' not found in {}", file_path, crate_key);
    }

    let content = std::fs::read_to_string(&full_path)?;
    let lines: Vec<&str> = content.lines().collect();
    let total = lines.len();

    let start_line = start.unwrap_or(1).max(1);

    // If no end specified and file is large, cap at MAX_DEFAULT_LINES
    let (end_line, was_truncated) = match end {
        Some(e) => (e.min(total), false),
        None => {
            let default_end = (start_line + MAX_DEFAULT_LINES - 1).min(total);
            let truncated = default_end < total;
            (default_end, truncated)
        }
    };

    let mut output = format!("{}:{} ({} total lines)\n\n", crate_key, file_path, total);

    for (i, line) in lines.iter().enumerate() {
        let line_num = i + 1;
        if line_num >= start_line && line_num <= end_line {
            output.push_str(&format!("{:4} | {}\n", line_num, line));
        }
    }

    if was_truncated {
        output.push_str(&format!(
            "\n[OUTPUT TRUNCATED] Showing lines {}-{} of {}. Use start_line/end_line parameters to read more.",
            start_line, end_line, total
        ));
    } else if end_line < total {
        output.push_str(&format!("\n... {} more lines", total - end_line));
    }

    Ok(output)
}

fn do_read_readme(crate_name: &str) -> anyhow::Result<String> {
    let db = Database::open()?;
    let crate_key = ensure_crate(&db, crate_name)?;
    let crate_path = db.get_crate_path(&crate_key)?
        .ok_or_else(|| anyhow::anyhow!("Crate path not found"))?;

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
            return Ok(format!("── {} ({}) ──\n\n{}", crate_key, name, content));
        }
    }

    anyhow::bail!("No README found in {}", crate_key)
}

fn truncate(s: &str, max: usize) -> String {
    if s.len() <= max {
        return s.to_string();
    }
    // Find a safe UTF-8 boundary
    let target = max.saturating_sub(3);
    match s.char_indices().nth(target) {
        Some((idx, _)) => format!("{}...", &s[..idx]),
        None => s.to_string(),
    }
}

async fn do_semantic_search(crate_name: &str, query: &str, limit: usize) -> anyhow::Result<String> {
    // Phase 1: Get all crate keys including re-exports (synchronous)
    let crate_keys = {
        let db = Database::open()?;
        let main_key = ensure_crate(&db, crate_name)?;
        get_crate_keys_with_reexports(&db, &main_key)?
    };

    if crate_keys.is_empty() {
        return Ok(format!("No crates found for: {}", crate_name));
    }

    // Phase 2: Generate embeddings for all crates that need them
    for key in &crate_keys {
        let has_embeddings = {
            let db = Database::open()?;
            db.has_embeddings(key)?
        };
        if !has_embeddings {
            generate_embeddings_for_crate(key).await?;
        }
    }

    // Phase 3: Get all embeddings from all crates
    let stored_embeddings = {
        let db = Database::open()?;
        let mut all_embeddings = Vec::new();
        for key in &crate_keys {
            all_embeddings.extend(db.get_all_embeddings(key)?);
        }
        all_embeddings
    };

    // Phase 4: Async embedding operations
    let embedder = EmbeddingManager::new()?;
    let query_embedding = embedder.embed_query(query).await?;

    // Phase 5: Compute similarities (in-memory, parallelized)
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

    // Phase 6: Format output
    if results.is_empty() {
        return Ok(format!("No results found for query: {}", query));
    }

    let main_crate = &crate_keys[0];
    let total_crates = crate_keys.len();
    let header = if total_crates > 1 {
        format!("Semantic search results for '{}' in {} + {} re-exports:\n\n", query, main_crate, total_crates - 1)
    } else {
        format!("Semantic search results for '{}' in {}:\n\n", query, main_crate)
    };

    let mut output = header;
    for result in &results {
        output.push_str(&format!("[{}] {} in {} (score: {:.3})\n",
            result.item_id, result.item_type, result.crate_key, result.similarity));
        let text = truncate(result.text_content.lines().next().unwrap_or(""), 80);
        output.push_str(&format!("  {}\n\n", text));
    }

    output.push_str(&format!("Total: {} results", results.len()));
    Ok(output)
}

/// Get all crate keys including re-exports recursively
fn get_crate_keys_with_reexports(db: &Database, main_key: &str) -> anyhow::Result<Vec<String>> {
    const MAX_REEXPORT_DEPTH: usize = 5;
    const MAX_TOTAL_CRATES: usize = 50;

    let mut keys = vec![main_key.to_string()];
    let mut seen: HashSet<String> = HashSet::new();
    seen.insert(main_key.to_string());

    let mut to_process: Vec<(String, usize)> = vec![(main_key.to_string(), 0)];

    while let Some((key, depth)) = to_process.pop() {
        if depth >= MAX_REEXPORT_DEPTH || keys.len() >= MAX_TOTAL_CRATES {
            break;
        }

        for reexport in db.get_reexports(&key)? {
            if let Some(reexport_key) = db.find_crate_key(&reexport)? {
                if !seen.contains(&reexport_key) {
                    seen.insert(reexport_key.clone());
                    keys.push(reexport_key.clone());
                    to_process.push((reexport_key, depth + 1));

                    if keys.len() >= MAX_TOTAL_CRATES {
                        break;
                    }
                }
            }
        }
    }

    Ok(keys)
}

async fn generate_embeddings_for_crate(crate_key: &str) -> anyhow::Result<()> {
    // Phase 1: Collect items from database (synchronous)
    let (items_to_embed, crate_id) = {
        let db = Database::open()?;
        let mut items: Vec<(String, String, String)> = Vec::new(); // (id, type, text)

        // Functions
        for func in db.get_functions(crate_key)? {
            let mut text = func.signature.clone();
            if let Some(docs) = &func.docs {
                text.push_str(". ");
                text.push_str(docs);
            }
            items.push((func.id, "function".to_string(), text));
        }

        // Structs
        for s in db.get_structs(crate_key)? {
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
            items.push((s.id, "struct".to_string(), text));
        }

        // Enums
        for e in db.get_enums(crate_key)? {
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
            items.push((e.id, "enum".to_string(), text));
        }

        // Traits
        for t in db.get_traits(crate_key)? {
            let mut text = format!("trait {}", t.name);
            if let Some(docs) = &t.docs {
                text.push_str(". ");
                text.push_str(docs);
            }
            items.push((t.id, "trait".to_string(), text));
        }

        let crate_id = db.get_crate_id(crate_key)?.ok_or_else(|| anyhow::anyhow!("Crate not found"))?;
        (items, crate_id)
    };

    if items_to_embed.is_empty() {
        return Ok(());
    }

    // Phase 2: Generate embeddings (async)
    let embedder = EmbeddingManager::new()?;
    let texts: Vec<String> = items_to_embed.iter().map(|(_, _, t)| t.clone()).collect();
    let embeddings = embedder.embed_texts(&texts).await?;

    // Phase 3: Prepare and save embeddings (synchronous)
    let embeddings_to_store: Vec<(String, String, Vec<u8>, String)> = items_to_embed
        .into_iter()
        .zip(embeddings)
        .map(|((id, item_type, text), emb)| {
            let bytes = embedding_to_bytes(&emb);
            (id, item_type, bytes, text)
        })
        .collect();

    let db = Database::open()?;
    db.save_embeddings(crate_id, &embeddings_to_store)?;

    Ok(())
}
