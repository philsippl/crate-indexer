use anyhow::{Context, Result};
use rusqlite::{params, Connection, OptionalExtension};
use std::path::PathBuf;

const INDEX_DIR: &str = ".crate-indexer";
const DB_FILE: &str = "index.db";

#[derive(Debug, Clone)]
pub struct FunctionInfo {
    pub id: String,
    pub name: String,
    pub file: String,
    pub line: usize,
    pub end_line: Option<usize>,
    pub signature: String,
    pub docs: Option<String>,
}

#[derive(Debug, Clone)]
pub struct StructInfo {
    pub id: String,
    pub name: String,
    pub file: String,
    pub line: usize,
    pub end_line: Option<usize>,
    pub visibility: String,
    pub fields: Vec<FieldInfo>,
    pub docs: Option<String>,
}

#[derive(Debug, Clone)]
pub struct FieldInfo {
    pub name: String,
    pub type_str: String,
    pub visibility: String,
    pub docs: Option<String>,
}

#[derive(Debug, Clone)]
pub struct EnumInfo {
    pub id: String,
    pub name: String,
    pub file: String,
    pub line: usize,
    pub end_line: Option<usize>,
    pub visibility: String,
    pub variants: Vec<VariantInfo>,
    pub docs: Option<String>,
}

#[derive(Debug, Clone)]
pub struct VariantInfo {
    pub name: String,
    pub kind: String, // "unit", "tuple", "struct"
    pub fields: Option<String>, // For tuple/struct variants
    pub docs: Option<String>,
}

#[derive(Debug, Clone)]
pub struct TraitInfo {
    pub id: String,
    pub name: String,
    pub file: String,
    pub line: usize,
    pub end_line: Option<usize>,
    pub visibility: String,
    pub docs: Option<String>,
}

#[derive(Debug, Clone)]
pub struct MacroInfo {
    pub id: String,
    pub name: String,
    pub file: String,
    pub line: usize,
    pub end_line: Option<usize>,
    pub kind: String, // "declarative", "proc_macro", "derive", "attribute"
    pub docs: Option<String>,
}

#[derive(Debug, Clone)]
pub struct TypeAliasInfo {
    pub id: String,
    pub name: String,
    pub file: String,
    pub line: usize,
    pub type_str: String,
    pub visibility: String,
    pub docs: Option<String>,
}

#[derive(Debug, Clone)]
pub struct ConstantInfo {
    pub id: String,
    pub name: String,
    pub file: String,
    pub line: usize,
    pub kind: String, // "const" or "static"
    pub type_str: String,
    pub visibility: String,
    pub docs: Option<String>,
}

#[derive(Debug, Clone)]
pub struct ImplInfo {
    pub id: String,
    pub file: String,
    pub line: usize,
    pub end_line: Option<usize>,
    pub self_type: String,
    pub trait_name: Option<String>,
}

// Container for all indexed items from a crate
#[derive(Debug, Default)]
pub struct CrateItems {
    pub functions: Vec<FunctionInfo>,
    pub structs: Vec<StructInfo>,
    pub enums: Vec<EnumInfo>,
    pub traits: Vec<TraitInfo>,
    pub macros: Vec<MacroInfo>,
    pub type_aliases: Vec<TypeAliasInfo>,
    pub constants: Vec<ConstantInfo>,
    pub impls: Vec<ImplInfo>,
}

pub struct Database {
    conn: Connection,
}

impl Database {
    pub fn open() -> Result<Self> {
        let db_path = db_path();
        if let Some(parent) = db_path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        let conn = Connection::open(&db_path)
            .with_context(|| format!("Failed to open database at {:?}", db_path))?;

        let db = Self { conn };
        db.init_schema()?;
        Ok(db)
    }

    fn init_schema(&self) -> Result<()> {
        self.conn.execute_batch(
            "
            CREATE TABLE IF NOT EXISTS crates (
                id INTEGER PRIMARY KEY,
                key TEXT UNIQUE NOT NULL,
                path TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS functions (
                id TEXT PRIMARY KEY,
                crate_id INTEGER NOT NULL,
                name TEXT NOT NULL,
                file TEXT NOT NULL,
                line INTEGER NOT NULL,
                end_line INTEGER,
                signature TEXT NOT NULL,
                docs TEXT,
                FOREIGN KEY (crate_id) REFERENCES crates(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS structs (
                id TEXT PRIMARY KEY,
                crate_id INTEGER NOT NULL,
                name TEXT NOT NULL,
                file TEXT NOT NULL,
                line INTEGER NOT NULL,
                end_line INTEGER,
                visibility TEXT NOT NULL,
                docs TEXT,
                FOREIGN KEY (crate_id) REFERENCES crates(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS struct_fields (
                id INTEGER PRIMARY KEY,
                struct_id TEXT NOT NULL,
                name TEXT NOT NULL,
                type_str TEXT NOT NULL,
                visibility TEXT NOT NULL,
                docs TEXT,
                FOREIGN KEY (struct_id) REFERENCES structs(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS enums (
                id TEXT PRIMARY KEY,
                crate_id INTEGER NOT NULL,
                name TEXT NOT NULL,
                file TEXT NOT NULL,
                line INTEGER NOT NULL,
                end_line INTEGER,
                visibility TEXT NOT NULL,
                docs TEXT,
                FOREIGN KEY (crate_id) REFERENCES crates(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS enum_variants (
                id INTEGER PRIMARY KEY,
                enum_id TEXT NOT NULL,
                name TEXT NOT NULL,
                kind TEXT NOT NULL,
                fields TEXT,
                docs TEXT,
                FOREIGN KEY (enum_id) REFERENCES enums(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS traits (
                id TEXT PRIMARY KEY,
                crate_id INTEGER NOT NULL,
                name TEXT NOT NULL,
                file TEXT NOT NULL,
                line INTEGER NOT NULL,
                end_line INTEGER,
                visibility TEXT NOT NULL,
                docs TEXT,
                FOREIGN KEY (crate_id) REFERENCES crates(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS macros (
                id TEXT PRIMARY KEY,
                crate_id INTEGER NOT NULL,
                name TEXT NOT NULL,
                file TEXT NOT NULL,
                line INTEGER NOT NULL,
                end_line INTEGER,
                kind TEXT NOT NULL,
                docs TEXT,
                FOREIGN KEY (crate_id) REFERENCES crates(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS type_aliases (
                id TEXT PRIMARY KEY,
                crate_id INTEGER NOT NULL,
                name TEXT NOT NULL,
                file TEXT NOT NULL,
                line INTEGER NOT NULL,
                type_str TEXT NOT NULL,
                visibility TEXT NOT NULL,
                docs TEXT,
                FOREIGN KEY (crate_id) REFERENCES crates(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS constants (
                id TEXT PRIMARY KEY,
                crate_id INTEGER NOT NULL,
                name TEXT NOT NULL,
                file TEXT NOT NULL,
                line INTEGER NOT NULL,
                kind TEXT NOT NULL,
                type_str TEXT NOT NULL,
                visibility TEXT NOT NULL,
                docs TEXT,
                FOREIGN KEY (crate_id) REFERENCES crates(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS impls (
                id TEXT PRIMARY KEY,
                crate_id INTEGER NOT NULL,
                file TEXT NOT NULL,
                line INTEGER NOT NULL,
                end_line INTEGER,
                self_type TEXT NOT NULL,
                trait_name TEXT,
                FOREIGN KEY (crate_id) REFERENCES crates(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS reexports (
                crate_id INTEGER NOT NULL,
                reexported_crate TEXT NOT NULL,
                FOREIGN KEY (crate_id) REFERENCES crates(id) ON DELETE CASCADE,
                PRIMARY KEY (crate_id, reexported_crate)
            );

            CREATE INDEX IF NOT EXISTS idx_functions_crate ON functions(crate_id);
            CREATE INDEX IF NOT EXISTS idx_functions_name ON functions(name);
            CREATE INDEX IF NOT EXISTS idx_structs_crate ON structs(crate_id);
            CREATE INDEX IF NOT EXISTS idx_structs_name ON structs(name);
            CREATE INDEX IF NOT EXISTS idx_enums_crate ON enums(crate_id);
            CREATE INDEX IF NOT EXISTS idx_enums_name ON enums(name);
            CREATE INDEX IF NOT EXISTS idx_traits_crate ON traits(crate_id);
            CREATE INDEX IF NOT EXISTS idx_traits_name ON traits(name);
            CREATE INDEX IF NOT EXISTS idx_macros_crate ON macros(crate_id);
            CREATE INDEX IF NOT EXISTS idx_macros_name ON macros(name);
            CREATE INDEX IF NOT EXISTS idx_type_aliases_crate ON type_aliases(crate_id);
            CREATE INDEX IF NOT EXISTS idx_constants_crate ON constants(crate_id);
            CREATE INDEX IF NOT EXISTS idx_impls_crate ON impls(crate_id);
            CREATE INDEX IF NOT EXISTS idx_impls_self_type ON impls(self_type);
            CREATE INDEX IF NOT EXISTS idx_reexports_crate ON reexports(crate_id);
            ",
        )?;
        Ok(())
    }

    pub fn add_crate(&self, key: &str, path: &PathBuf, items: &CrateItems, reexports: &[String]) -> Result<()> {
        let tx = self.conn.unchecked_transaction()?;

        // Insert or replace crate
        tx.execute(
            "INSERT OR REPLACE INTO crates (key, path) VALUES (?, ?)",
            params![key, path.to_string_lossy()],
        )?;

        let crate_id: i64 = tx.query_row(
            "SELECT id FROM crates WHERE key = ?",
            [key],
            |row| row.get(0),
        )?;

        // Delete old data for this crate
        tx.execute("DELETE FROM functions WHERE crate_id = ?", [crate_id])?;
        tx.execute("DELETE FROM struct_fields WHERE struct_id IN (SELECT id FROM structs WHERE crate_id = ?)", [crate_id])?;
        tx.execute("DELETE FROM structs WHERE crate_id = ?", [crate_id])?;
        tx.execute("DELETE FROM enum_variants WHERE enum_id IN (SELECT id FROM enums WHERE crate_id = ?)", [crate_id])?;
        tx.execute("DELETE FROM enums WHERE crate_id = ?", [crate_id])?;
        tx.execute("DELETE FROM traits WHERE crate_id = ?", [crate_id])?;
        tx.execute("DELETE FROM macros WHERE crate_id = ?", [crate_id])?;
        tx.execute("DELETE FROM type_aliases WHERE crate_id = ?", [crate_id])?;
        tx.execute("DELETE FROM constants WHERE crate_id = ?", [crate_id])?;
        tx.execute("DELETE FROM impls WHERE crate_id = ?", [crate_id])?;
        tx.execute("DELETE FROM reexports WHERE crate_id = ?", [crate_id])?;

        // Insert functions
        {
            let mut stmt = tx.prepare(
                "INSERT INTO functions (id, crate_id, name, file, line, end_line, signature, docs)
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?)"
            )?;
            for func in &items.functions {
                stmt.execute(params![
                    func.id, crate_id, func.name, func.file,
                    func.line as i64, func.end_line.map(|l| l as i64),
                    func.signature, func.docs,
                ])?;
            }
        }

        // Insert structs and their fields
        {
            let mut struct_stmt = tx.prepare(
                "INSERT INTO structs (id, crate_id, name, file, line, end_line, visibility, docs)
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?)"
            )?;
            let mut field_stmt = tx.prepare(
                "INSERT INTO struct_fields (struct_id, name, type_str, visibility, docs)
                 VALUES (?, ?, ?, ?, ?)"
            )?;
            for s in &items.structs {
                struct_stmt.execute(params![
                    s.id, crate_id, s.name, s.file,
                    s.line as i64, s.end_line.map(|l| l as i64),
                    s.visibility, s.docs,
                ])?;
                for field in &s.fields {
                    field_stmt.execute(params![
                        s.id, field.name, field.type_str, field.visibility, field.docs,
                    ])?;
                }
            }
        }

        // Insert enums and their variants
        {
            let mut enum_stmt = tx.prepare(
                "INSERT INTO enums (id, crate_id, name, file, line, end_line, visibility, docs)
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?)"
            )?;
            let mut variant_stmt = tx.prepare(
                "INSERT INTO enum_variants (enum_id, name, kind, fields, docs)
                 VALUES (?, ?, ?, ?, ?)"
            )?;
            for e in &items.enums {
                enum_stmt.execute(params![
                    e.id, crate_id, e.name, e.file,
                    e.line as i64, e.end_line.map(|l| l as i64),
                    e.visibility, e.docs,
                ])?;
                for variant in &e.variants {
                    variant_stmt.execute(params![
                        e.id, variant.name, variant.kind, variant.fields, variant.docs,
                    ])?;
                }
            }
        }

        // Insert traits
        {
            let mut stmt = tx.prepare(
                "INSERT INTO traits (id, crate_id, name, file, line, end_line, visibility, docs)
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?)"
            )?;
            for t in &items.traits {
                stmt.execute(params![
                    t.id, crate_id, t.name, t.file,
                    t.line as i64, t.end_line.map(|l| l as i64),
                    t.visibility, t.docs,
                ])?;
            }
        }

        // Insert macros
        {
            let mut stmt = tx.prepare(
                "INSERT INTO macros (id, crate_id, name, file, line, end_line, kind, docs)
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?)"
            )?;
            for m in &items.macros {
                stmt.execute(params![
                    m.id, crate_id, m.name, m.file,
                    m.line as i64, m.end_line.map(|l| l as i64),
                    m.kind, m.docs,
                ])?;
            }
        }

        // Insert type aliases
        {
            let mut stmt = tx.prepare(
                "INSERT INTO type_aliases (id, crate_id, name, file, line, type_str, visibility, docs)
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?)"
            )?;
            for t in &items.type_aliases {
                stmt.execute(params![
                    t.id, crate_id, t.name, t.file,
                    t.line as i64, t.type_str, t.visibility, t.docs,
                ])?;
            }
        }

        // Insert constants
        {
            let mut stmt = tx.prepare(
                "INSERT INTO constants (id, crate_id, name, file, line, kind, type_str, visibility, docs)
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)"
            )?;
            for c in &items.constants {
                stmt.execute(params![
                    c.id, crate_id, c.name, c.file,
                    c.line as i64, c.kind, c.type_str, c.visibility, c.docs,
                ])?;
            }
        }

        // Insert impls
        {
            let mut stmt = tx.prepare(
                "INSERT INTO impls (id, crate_id, file, line, end_line, self_type, trait_name)
                 VALUES (?, ?, ?, ?, ?, ?, ?)"
            )?;
            for i in &items.impls {
                stmt.execute(params![
                    i.id, crate_id, i.file,
                    i.line as i64, i.end_line.map(|l| l as i64),
                    i.self_type, i.trait_name,
                ])?;
            }
        }

        // Insert reexports
        {
            let mut stmt = tx.prepare(
                "INSERT INTO reexports (crate_id, reexported_crate) VALUES (?, ?)"
            )?;
            for reexport in reexports {
                stmt.execute(params![crate_id, reexport])?;
            }
        }

        tx.commit()?;
        Ok(())
    }

    pub fn get_crate_path(&self, key: &str) -> Result<Option<PathBuf>> {
        let mut stmt = self.conn.prepare("SELECT path FROM crates WHERE key = ?")?;
        let path = stmt.query_row([key], |row| {
            let path: String = row.get(0)?;
            Ok(PathBuf::from(path))
        }).optional()?;
        Ok(path)
    }

    pub fn get_reexports(&self, key: &str) -> Result<Vec<String>> {
        let mut stmt = self.conn.prepare(
            "SELECT r.reexported_crate FROM reexports r
             JOIN crates c ON c.id = r.crate_id
             WHERE c.key = ?"
        )?;
        let reexports = stmt.query_map([key], |row| row.get(0))?
            .collect::<std::result::Result<Vec<String>, _>>()?;
        Ok(reexports)
    }

    pub fn list_crate_keys(&self) -> Result<Vec<String>> {
        let mut stmt = self.conn.prepare("SELECT key FROM crates")?;
        let keys = stmt.query_map([], |row| row.get(0))?
            .collect::<std::result::Result<Vec<String>, _>>()?;
        Ok(keys)
    }

    pub fn find_crate_key(&self, name: &str) -> Result<Option<String>> {
        let mut stmt = self.conn.prepare("SELECT key FROM crates WHERE key = ?")?;
        if let Some(key) = stmt.query_row([name], |row| row.get::<_, String>(0)).optional()? {
            return Ok(Some(key));
        }

        let mut stmt = self.conn.prepare(
            "SELECT key FROM crates WHERE key GLOB ? ORDER BY key"
        )?;
        let pattern = format!("{}-[0-9]*", name);
        let matches: Vec<String> = stmt.query_map([&pattern], |row| row.get(0))?
            .collect::<std::result::Result<_, _>>()?;

        match matches.len() {
            0 => Ok(None),
            1 => Ok(Some(matches.into_iter().next().unwrap())),
            _ => anyhow::bail!("Multiple versions found for '{}': {:?}", name, matches),
        }
    }

    // Query functions
    pub fn get_functions(&self, crate_key: &str) -> Result<Vec<FunctionInfo>> {
        let mut stmt = self.conn.prepare(
            "SELECT f.id, f.name, f.file, f.line, f.end_line, f.signature, f.docs
             FROM functions f JOIN crates c ON c.id = f.crate_id WHERE c.key = ?"
        )?;
        let rows = stmt.query_map([crate_key], |row| {
            Ok(FunctionInfo {
                id: row.get(0)?, name: row.get(1)?, file: row.get(2)?,
                line: row.get::<_, i64>(3)? as usize,
                end_line: row.get::<_, Option<i64>>(4)?.map(|l| l as usize),
                signature: row.get(5)?, docs: row.get(6)?,
            })
        })?;
        rows.collect::<std::result::Result<Vec<_>, _>>().map_err(Into::into)
    }

    pub fn get_function_by_id(&self, id: &str) -> Result<Option<(String, FunctionInfo)>> {
        let mut stmt = self.conn.prepare(
            "SELECT c.key, f.id, f.name, f.file, f.line, f.end_line, f.signature, f.docs
             FROM functions f JOIN crates c ON c.id = f.crate_id WHERE f.id = ?"
        )?;
        stmt.query_row([id], |row| {
            Ok((row.get::<_, String>(0)?, FunctionInfo {
                id: row.get(1)?, name: row.get(2)?, file: row.get(3)?,
                line: row.get::<_, i64>(4)? as usize,
                end_line: row.get::<_, Option<i64>>(5)?.map(|l| l as usize),
                signature: row.get(6)?, docs: row.get(7)?,
            }))
        }).optional().map_err(Into::into)
    }

    // Query structs
    pub fn get_structs(&self, crate_key: &str) -> Result<Vec<StructInfo>> {
        let mut stmt = self.conn.prepare(
            "SELECT s.id, s.name, s.file, s.line, s.end_line, s.visibility, s.docs
             FROM structs s JOIN crates c ON c.id = s.crate_id WHERE c.key = ?"
        )?;
        let structs: Vec<(String, String, String, usize, Option<usize>, String, Option<String>)> = stmt.query_map([crate_key], |row| {
            Ok((
                row.get(0)?, row.get(1)?, row.get(2)?,
                row.get::<_, i64>(3)? as usize,
                row.get::<_, Option<i64>>(4)?.map(|l| l as usize),
                row.get(5)?, row.get(6)?,
            ))
        })?.collect::<std::result::Result<_, _>>()?;

        let mut result = Vec::new();
        for (id, name, file, line, end_line, visibility, docs) in structs {
            let fields = self.get_struct_fields(&id)?;
            result.push(StructInfo { id, name, file, line, end_line, visibility, fields, docs });
        }
        Ok(result)
    }

    fn get_struct_fields(&self, struct_id: &str) -> Result<Vec<FieldInfo>> {
        let mut stmt = self.conn.prepare(
            "SELECT name, type_str, visibility, docs FROM struct_fields WHERE struct_id = ?"
        )?;
        let rows = stmt.query_map([struct_id], |row| {
            Ok(FieldInfo {
                name: row.get(0)?, type_str: row.get(1)?,
                visibility: row.get(2)?, docs: row.get(3)?,
            })
        })?;
        rows.collect::<std::result::Result<Vec<_>, _>>().map_err(Into::into)
    }

    pub fn get_struct_by_id(&self, id: &str) -> Result<Option<(String, StructInfo)>> {
        let mut stmt = self.conn.prepare(
            "SELECT c.key, s.id, s.name, s.file, s.line, s.end_line, s.visibility, s.docs
             FROM structs s JOIN crates c ON c.id = s.crate_id WHERE s.id = ?"
        )?;
        let result = stmt.query_row([id], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?, row.get::<_, String>(2)?, row.get::<_, String>(3)?,
                row.get::<_, i64>(4)? as usize,
                row.get::<_, Option<i64>>(5)?.map(|l| l as usize),
                row.get::<_, String>(6)?, row.get::<_, Option<String>>(7)?,
            ))
        }).optional()?;

        match result {
            Some((crate_key, id, name, file, line, end_line, visibility, docs)) => {
                let fields = self.get_struct_fields(&id)?;
                Ok(Some((crate_key, StructInfo { id, name, file, line, end_line, visibility, fields, docs })))
            }
            None => Ok(None),
        }
    }

    // Query enums
    pub fn get_enums(&self, crate_key: &str) -> Result<Vec<EnumInfo>> {
        let mut stmt = self.conn.prepare(
            "SELECT e.id, e.name, e.file, e.line, e.end_line, e.visibility, e.docs
             FROM enums e JOIN crates c ON c.id = e.crate_id WHERE c.key = ?"
        )?;
        let enums: Vec<(String, String, String, usize, Option<usize>, String, Option<String>)> = stmt.query_map([crate_key], |row| {
            Ok((
                row.get(0)?, row.get(1)?, row.get(2)?,
                row.get::<_, i64>(3)? as usize,
                row.get::<_, Option<i64>>(4)?.map(|l| l as usize),
                row.get(5)?, row.get(6)?,
            ))
        })?.collect::<std::result::Result<_, _>>()?;

        let mut result = Vec::new();
        for (id, name, file, line, end_line, visibility, docs) in enums {
            let variants = self.get_enum_variants(&id)?;
            result.push(EnumInfo { id, name, file, line, end_line, visibility, variants, docs });
        }
        Ok(result)
    }

    fn get_enum_variants(&self, enum_id: &str) -> Result<Vec<VariantInfo>> {
        let mut stmt = self.conn.prepare(
            "SELECT name, kind, fields, docs FROM enum_variants WHERE enum_id = ?"
        )?;
        let rows = stmt.query_map([enum_id], |row| {
            Ok(VariantInfo {
                name: row.get(0)?, kind: row.get(1)?,
                fields: row.get(2)?, docs: row.get(3)?,
            })
        })?;
        rows.collect::<std::result::Result<Vec<_>, _>>().map_err(Into::into)
    }

    pub fn get_enum_by_id(&self, id: &str) -> Result<Option<(String, EnumInfo)>> {
        let mut stmt = self.conn.prepare(
            "SELECT c.key, e.id, e.name, e.file, e.line, e.end_line, e.visibility, e.docs
             FROM enums e JOIN crates c ON c.id = e.crate_id WHERE e.id = ?"
        )?;
        let result = stmt.query_row([id], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?, row.get::<_, String>(2)?, row.get::<_, String>(3)?,
                row.get::<_, i64>(4)? as usize,
                row.get::<_, Option<i64>>(5)?.map(|l| l as usize),
                row.get::<_, String>(6)?, row.get::<_, Option<String>>(7)?,
            ))
        }).optional()?;

        match result {
            Some((crate_key, id, name, file, line, end_line, visibility, docs)) => {
                let variants = self.get_enum_variants(&id)?;
                Ok(Some((crate_key, EnumInfo { id, name, file, line, end_line, visibility, variants, docs })))
            }
            None => Ok(None),
        }
    }

    // Query traits
    pub fn get_traits(&self, crate_key: &str) -> Result<Vec<TraitInfo>> {
        let mut stmt = self.conn.prepare(
            "SELECT t.id, t.name, t.file, t.line, t.end_line, t.visibility, t.docs
             FROM traits t JOIN crates c ON c.id = t.crate_id WHERE c.key = ?"
        )?;
        let rows = stmt.query_map([crate_key], |row| {
            Ok(TraitInfo {
                id: row.get(0)?, name: row.get(1)?, file: row.get(2)?,
                line: row.get::<_, i64>(3)? as usize,
                end_line: row.get::<_, Option<i64>>(4)?.map(|l| l as usize),
                visibility: row.get(5)?, docs: row.get(6)?,
            })
        })?;
        rows.collect::<std::result::Result<Vec<_>, _>>().map_err(Into::into)
    }

    pub fn get_trait_by_id(&self, id: &str) -> Result<Option<(String, TraitInfo)>> {
        let mut stmt = self.conn.prepare(
            "SELECT c.key, t.id, t.name, t.file, t.line, t.end_line, t.visibility, t.docs
             FROM traits t JOIN crates c ON c.id = t.crate_id WHERE t.id = ?"
        )?;
        stmt.query_row([id], |row| {
            Ok((row.get::<_, String>(0)?, TraitInfo {
                id: row.get(1)?, name: row.get(2)?, file: row.get(3)?,
                line: row.get::<_, i64>(4)? as usize,
                end_line: row.get::<_, Option<i64>>(5)?.map(|l| l as usize),
                visibility: row.get(6)?, docs: row.get(7)?,
            }))
        }).optional().map_err(Into::into)
    }

    // Query macros
    pub fn get_macros(&self, crate_key: &str) -> Result<Vec<MacroInfo>> {
        let mut stmt = self.conn.prepare(
            "SELECT m.id, m.name, m.file, m.line, m.end_line, m.kind, m.docs
             FROM macros m JOIN crates c ON c.id = m.crate_id WHERE c.key = ?"
        )?;
        let rows = stmt.query_map([crate_key], |row| {
            Ok(MacroInfo {
                id: row.get(0)?, name: row.get(1)?, file: row.get(2)?,
                line: row.get::<_, i64>(3)? as usize,
                end_line: row.get::<_, Option<i64>>(4)?.map(|l| l as usize),
                kind: row.get(5)?, docs: row.get(6)?,
            })
        })?;
        rows.collect::<std::result::Result<Vec<_>, _>>().map_err(Into::into)
    }

    pub fn get_macro_by_id(&self, id: &str) -> Result<Option<(String, MacroInfo)>> {
        let mut stmt = self.conn.prepare(
            "SELECT c.key, m.id, m.name, m.file, m.line, m.end_line, m.kind, m.docs
             FROM macros m JOIN crates c ON c.id = m.crate_id WHERE m.id = ?"
        )?;
        stmt.query_row([id], |row| {
            Ok((row.get::<_, String>(0)?, MacroInfo {
                id: row.get(1)?, name: row.get(2)?, file: row.get(3)?,
                line: row.get::<_, i64>(4)? as usize,
                end_line: row.get::<_, Option<i64>>(5)?.map(|l| l as usize),
                kind: row.get(6)?, docs: row.get(7)?,
            }))
        }).optional().map_err(Into::into)
    }

    // Query type aliases
    pub fn get_type_aliases(&self, crate_key: &str) -> Result<Vec<TypeAliasInfo>> {
        let mut stmt = self.conn.prepare(
            "SELECT t.id, t.name, t.file, t.line, t.type_str, t.visibility, t.docs
             FROM type_aliases t JOIN crates c ON c.id = t.crate_id WHERE c.key = ?"
        )?;
        let rows = stmt.query_map([crate_key], |row| {
            Ok(TypeAliasInfo {
                id: row.get(0)?, name: row.get(1)?, file: row.get(2)?,
                line: row.get::<_, i64>(3)? as usize,
                type_str: row.get(4)?, visibility: row.get(5)?, docs: row.get(6)?,
            })
        })?;
        rows.collect::<std::result::Result<Vec<_>, _>>().map_err(Into::into)
    }

    pub fn get_type_alias_by_id(&self, id: &str) -> Result<Option<(String, TypeAliasInfo)>> {
        let mut stmt = self.conn.prepare(
            "SELECT c.key, t.id, t.name, t.file, t.line, t.type_str, t.visibility, t.docs
             FROM type_aliases t JOIN crates c ON c.id = t.crate_id WHERE t.id = ?"
        )?;
        stmt.query_row([id], |row| {
            Ok((row.get::<_, String>(0)?, TypeAliasInfo {
                id: row.get(1)?, name: row.get(2)?, file: row.get(3)?,
                line: row.get::<_, i64>(4)? as usize,
                type_str: row.get(5)?, visibility: row.get(6)?, docs: row.get(7)?,
            }))
        }).optional().map_err(Into::into)
    }

    // Query constants
    pub fn get_constants(&self, crate_key: &str) -> Result<Vec<ConstantInfo>> {
        let mut stmt = self.conn.prepare(
            "SELECT c2.id, c2.name, c2.file, c2.line, c2.kind, c2.type_str, c2.visibility, c2.docs
             FROM constants c2 JOIN crates c ON c.id = c2.crate_id WHERE c.key = ?"
        )?;
        let rows = stmt.query_map([crate_key], |row| {
            Ok(ConstantInfo {
                id: row.get(0)?, name: row.get(1)?, file: row.get(2)?,
                line: row.get::<_, i64>(3)? as usize,
                kind: row.get(4)?, type_str: row.get(5)?,
                visibility: row.get(6)?, docs: row.get(7)?,
            })
        })?;
        rows.collect::<std::result::Result<Vec<_>, _>>().map_err(Into::into)
    }

    pub fn get_constant_by_id(&self, id: &str) -> Result<Option<(String, ConstantInfo)>> {
        let mut stmt = self.conn.prepare(
            "SELECT c.key, c2.id, c2.name, c2.file, c2.line, c2.kind, c2.type_str, c2.visibility, c2.docs
             FROM constants c2 JOIN crates c ON c.id = c2.crate_id WHERE c2.id = ?"
        )?;
        stmt.query_row([id], |row| {
            Ok((row.get::<_, String>(0)?, ConstantInfo {
                id: row.get(1)?, name: row.get(2)?, file: row.get(3)?,
                line: row.get::<_, i64>(4)? as usize,
                kind: row.get(5)?, type_str: row.get(6)?,
                visibility: row.get(7)?, docs: row.get(8)?,
            }))
        }).optional().map_err(Into::into)
    }

    // Query impls
    pub fn get_impls(&self, crate_key: &str) -> Result<Vec<ImplInfo>> {
        let mut stmt = self.conn.prepare(
            "SELECT i.id, i.file, i.line, i.end_line, i.self_type, i.trait_name
             FROM impls i JOIN crates c ON c.id = i.crate_id WHERE c.key = ?"
        )?;
        let rows = stmt.query_map([crate_key], |row| {
            Ok(ImplInfo {
                id: row.get(0)?, file: row.get(1)?,
                line: row.get::<_, i64>(2)? as usize,
                end_line: row.get::<_, Option<i64>>(3)?.map(|l| l as usize),
                self_type: row.get(4)?, trait_name: row.get(5)?,
            })
        })?;
        rows.collect::<std::result::Result<Vec<_>, _>>().map_err(Into::into)
    }

    pub fn get_impl_by_id(&self, id: &str) -> Result<Option<(String, ImplInfo)>> {
        let mut stmt = self.conn.prepare(
            "SELECT c.key, i.id, i.file, i.line, i.end_line, i.self_type, i.trait_name
             FROM impls i JOIN crates c ON c.id = i.crate_id WHERE i.id = ?"
        )?;
        stmt.query_row([id], |row| {
            Ok((row.get::<_, String>(0)?, ImplInfo {
                id: row.get(1)?, file: row.get(2)?,
                line: row.get::<_, i64>(3)? as usize,
                end_line: row.get::<_, Option<i64>>(4)?.map(|l| l as usize),
                self_type: row.get(5)?, trait_name: row.get(6)?,
            }))
        }).optional().map_err(Into::into)
    }
}

pub fn index_dir() -> PathBuf {
    dirs::home_dir()
        .unwrap_or_else(|| PathBuf::from("."))
        .join(INDEX_DIR)
}

pub fn db_path() -> PathBuf {
    index_dir().join(DB_FILE)
}

pub fn crates_dir() -> PathBuf {
    index_dir().join("crates")
}

pub fn crate_path(name: &str, version: &str) -> PathBuf {
    crates_dir().join(format!("{}-{}", name, version))
}
