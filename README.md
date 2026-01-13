# crate-indexer

A CLI tool and MCP server for fetching, indexing, and searching Rust crates from crates.io.

## Features

- Fetch and index crates from crates.io with automatic dependency resolution
- Search through source code with regex patterns
- Browse and search functions, structs, enums, traits, macros, type aliases, constants, and impl blocks
- Automatic update checking - always uses the latest crate version
- MCP server mode for AI assistant integration (Claude, etc.)
- Parallel downloading and indexing for fast operation

## Installation

```bash
cargo install crate-indexer
```

Or build from source:

```bash
git clone https://github.com/philsippl/crate-indexer
cd crate-indexer
cargo install --path .
```

## CLI Usage

### Fetch a crate

```bash
# Fetch latest version (automatically fetches re-exported dependencies)
crate-indexer fetch serde

# Fetch specific version
crate-indexer fetch serde --version 1.0.200
```

### Search source code

```bash
# Regex search through crate source
crate-indexer search serde "impl.*Serialize"
```

### Browse definitions

```bash
# List all functions (or filter with regex)
crate-indexer functions serde
crate-indexer functions serde "serialize"

# List structs, enums, traits, etc.
crate-indexer structs serde
crate-indexer enums serde "Error"
crate-indexer traits serde
crate-indexer macros serde
crate-indexer types serde
crate-indexer consts serde
crate-indexer impls serde "Serialize"
```

### View item details

Each item has an 8-character hex ID shown in brackets. Use it to view full details:

```bash
crate-indexer show a1b2c3d4
```

### Read source files

```bash
# Read entire file
crate-indexer read serde src/lib.rs

# Read specific lines
crate-indexer read serde src/ser.rs --start 100 --end 150
```

### View README

```bash
crate-indexer readme serde
```

### Check latest version

```bash
crate-indexer latest serde
```

## Automatic Updates

When you query a crate by name (e.g., `serde`), the tool automatically checks crates.io for the latest version. If a newer version is available, it fetches and indexes it before returning results.

To use a specific version without update checking, specify the full version:

```bash
crate-indexer functions serde-1.0.200
```

## MCP Server

Run as an MCP (Model Context Protocol) server for AI assistant integration:

```bash
crate-indexer mcp
```

### Claude Desktop Configuration

Add to your Claude Desktop config (`~/Library/Application Support/Claude/claude_desktop_config.json` on macOS):

```json
{
  "mcpServers": {
    "crate-indexer": {
      "command": "crate-indexer",
      "args": ["mcp"]
    }
  }
}
```

Note: If `crate-indexer` is not in your PATH, use the full path (e.g., `~/.cargo/bin/crate-indexer`).

### Available MCP Tools

| Tool | Description |
|------|-------------|
| `fetch_crate` | Download and index a crate from crates.io |
| `search_crate` | Regex search through crate source code |
| `list_functions` | List/search function definitions |
| `list_structs` | List/search struct definitions |
| `list_enums` | List/search enum definitions |
| `list_traits` | List/search trait definitions |
| `list_impls` | List/search impl blocks |
| `show_item` | Get detailed info and source code for an item by ID |
| `read_file` | Read files from indexed crates |
| `read_readme` | Get the README of a crate |

## Data Storage

Indexed data is stored in `~/.crate-indexer/`:
- `crates/` - Downloaded and extracted crate sources
- `index.db` - SQLite database with indexed definitions

## License

MIT
