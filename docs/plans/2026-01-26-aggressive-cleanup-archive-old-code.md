# Aggressive Cleanup: Archive Old Code Design

**Goal:** Remove all old/deprecated code by moving it to `./archive/` to eliminate compilation errors and provide a clean foundation for rebuilding with the new DSL system.

**Strategy:** Archive entire subsystems that depend on old model types (expr, fact, query, old planner), keeping only the new DSL-based system (parser, lowering, new model types, UnifiedGraph, cache).

**Outcome:** Codebase compiles cleanly with ~30-40 files archived. Clean slate for rebuilding query planner with UnifiedGraph integration.

---

## What Gets Archived

### Old Model System (src/model/)

**Archive these files:**
- `expr.rs` - Old expression AST (29KB, replaced by DSL SQL expressions)
- `fact.rs` - Old fact-based model (27KB, replaced by tables/measures)
- `query.rs` - Old query model (43KB, will rebuild with UnifiedGraph)
- `pivot_report.rs` - Old pivot reports (11KB, replaced by DSL reports)
- `source.rs` - Old source definitions (12KB, replaced by table sources)
- `dimension_role.rs` - Old dimension roles (16KB, replaced by DSL dimensions)
- `loader/` - Old Lua-based loader directory (entire directory)
- `emitter/` - Old model emitters directory (entire directory)

**Keep these files:**
- `mod.rs` - Module declarations (will update to remove archived modules)
- `calendar.rs` - New DSL calendar types
- `defaults.rs` - New DSL defaults
- `dimension.rs` - New DSL dimension types
- `measure.rs` - New DSL measure types
- `report.rs` - New DSL report types
- `table.rs` - New DSL table types
- `types.rs` - New common types (Cardinality, etc.)

### Old Graph System (src/semantic/)

**Archive these directories/files:**
- `model_graph/` - Entire old ModelGraph directory (4 files, entity-level graph)
  - `mod.rs`
  - `async_graph.rs`
  - `resolution.rs`
  - `tests.rs`
- `column_lineage.rs` - Old column lineage tracking (integrated into UnifiedGraph)
- `executor.rs` - Old query executor (will rebuild)
- `semantic_model.rs` - Old semantic model wrapper

**Keep these:**
- `graph/` - New UnifiedGraph (complete, tested, documented)
- `inference/` - Inference engine (still needed)
- `error.rs` - Error types (may need updating)
- `mod.rs` - Module declarations (will update)

### Old Query Planner (src/semantic/planner/)

**Archive entire directory:**
- `planner/` - Complete old query planner
  - `mod.rs` - Main planner module
  - `emit.rs` - SQL emission (30KB)
  - `emit_multi.rs` - Multi-fact emission (18KB)
  - `emit_time.rs` - Time intelligence emission (13KB)
  - `logical.rs` - Logical planning (14KB)
  - `prune.rs` - Query pruning (14KB)
  - `resolve.rs` - Resolution logic (22KB)
  - `resolved.rs` - Resolved query types (10KB)
  - `types.rs` - Planner types (13KB)
  - `validate.rs` - Query validation (19KB)
  - `report/` - Report planning subdirectory
    - `planner.rs`
    - `pivot_planner.rs`
    - `pivot_emitter.rs`
    - `emitter.rs`
    - `tests.rs`

**Rationale:** The entire planner is tightly coupled to the old model types (expr, fact, query). Will rebuild from scratch using UnifiedGraph.

### Old Integrations

**Archive these files:**
- `src/translation/mod.rs` - Old translation layer (depends on old planner)
- `src/web/server.rs` - Web server (depends on old planner/executor)
- `src/lsp/project.rs` - LSP integration (uses old loader)
- `src/metadata/types.rs` - Old metadata types (SourceEntity, SourceColumn)

**Keep these:**
- `src/metadata/mod.rs` - Core metadata provider (may need updates)
- `src/metadata/provider.rs` - Database introspection (still needed)
- `src/lsp/` - Other LSP modules (will need updating)

### Other Modules to Review

**Keep (core infrastructure):**
- `src/sql/` - SQL AST and generation (still needed)
- `src/config/` - Configuration (still needed)
- `src/crypto/` - Encryption utilities (still needed)
- `src/dsl/` - DSL parser (keep, this is new)
- `src/lowering/` - DSL → Model translator (keep, this is new)
- `src/cache/` - Graph cache (keep, newly built)

---

## Archive Directory Structure

Create this structure in `./archive/`:

```
archive/
├── README.md                          # Explanation of archived code
├── model/
│   ├── expr.rs
│   ├── fact.rs
│   ├── query.rs
│   ├── pivot_report.rs
│   ├── source.rs
│   ├── dimension_role.rs
│   ├── loader/
│   │   ├── mod.rs
│   │   ├── lua.rs
│   │   └── ... (other loader files)
│   └── emitter/
│       ├── mod.rs
│       ├── source.rs
│       └── ... (other emitter files)
├── semantic/
│   ├── model_graph/
│   │   ├── mod.rs
│   │   ├── async_graph.rs
│   │   ├── resolution.rs
│   │   └── tests.rs
│   ├── planner/
│   │   ├── mod.rs
│   │   ├── emit.rs
│   │   ├── emit_multi.rs
│   │   ├── emit_time.rs
│   │   ├── logical.rs
│   │   ├── prune.rs
│   │   ├── resolve.rs
│   │   ├── resolved.rs
│   │   ├── types.rs
│   │   ├── validate.rs
│   │   └── report/
│   │       ├── planner.rs
│   │       ├── pivot_planner.rs
│   │       ├── pivot_emitter.rs
│   │       ├── emitter.rs
│   │       └── tests.rs
│   ├── column_lineage.rs
│   ├── executor.rs
│   └── semantic_model.rs
├── translation/
│   └── mod.rs
├── web/
│   └── server.rs
├── lsp/
│   └── project.rs
└── metadata/
    └── types.rs
```

### Archive README

Create `archive/README.md`:

```markdown
# Archived Code

This directory contains code from the old model system that was archived on 2026-01-26 during the transition to the new DSL-based architecture.

## Why Archived?

The codebase underwent a major refactoring to replace:
- **Old Model**: Lua-based, fact-oriented, expression AST
- **Old Graph**: Entity-level ModelGraph
- **Old Planner**: Built on old model types

With:
- **New Model**: DSL-based (atoms, times, slicers), typed in Rust
- **New Graph**: Column-level UnifiedGraph with inference integration
- **New Planner**: To be rebuilt using UnifiedGraph

## What's Here

- `model/` - Old model types (expr, fact, query, etc.) and Lua loader
- `semantic/` - Old ModelGraph, planner, column lineage, executor
- `translation/` - Old translation layer
- `web/` - Old web server (depended on old planner)
- `lsp/` - Old LSP project integration
- `metadata/` - Old metadata type definitions

## Restoration

If you need to reference this code:
1. It's preserved exactly as it was on 2026-01-26
2. Check git history for commit: [will be filled in]
3. The design docs explain the new architecture:
   - `docs/plans/2025-01-25-graph-cache-integration-design.md`
   - `docs/architecture/unified-graph.md`
   - `docs/semantic-model-dsl-spec-v3.1.md`

## New Architecture

The new system flow:
```
DSL text → [parser] → AST → [lowering] → Model → [UnifiedGraph builder] → UnifiedGraph → [new planner] → SQL
```

Active code:
- `src/dsl/` - DSL parser
- `src/lowering/` - AST → Model translator
- `src/model/` - New model types (calendar, dimension, table, measure, report)
- `src/semantic/graph/` - UnifiedGraph
- `src/cache/` - Graph cache system
```

---

## Updated Module Structure

After archiving, `src/lib.rs` will export:

```rust
// Core DSL and Model
pub mod dsl;           // DSL parser (lexer, parser, AST)
pub mod lowering;      // DSL → Model translator
pub mod model;         // Model types (calendar, dimension, table, measure, report)

// Semantic Layer
pub mod semantic {
    pub mod graph;     // UnifiedGraph (column-level)
    pub mod inference; // Relationship inference
    pub mod error;     // Semantic errors
}

// Infrastructure
pub mod cache;         // Graph cache (two-tier)
pub mod metadata;      // Database introspection
pub mod sql;           // SQL AST and generation
pub mod config;        // Configuration
pub mod crypto;        // Encryption

// LSP (needs updating)
pub mod lsp;           // Language Server Protocol support

// TO BE REBUILT:
// - Query planner (using UnifiedGraph)
// - Translation layer (using new planner)
// - Executor (using new planner)
// - Web server (using new planner)
```

---

## Implementation Steps

### Step 1: Create Archive Structure

```bash
mkdir -p archive/model/loader archive/model/emitter
mkdir -p archive/semantic/model_graph archive/semantic/planner/report
mkdir -p archive/translation archive/web archive/lsp archive/metadata
```

### Step 2: Move Model Files

```bash
# Old model types
mv src/model/expr.rs archive/model/
mv src/model/fact.rs archive/model/
mv src/model/query.rs archive/model/
mv src/model/pivot_report.rs archive/model/
mv src/model/source.rs archive/model/
mv src/model/dimension_role.rs archive/model/

# Old loader and emitter
mv src/model/loader/* archive/model/loader/
rmdir src/model/loader
mv src/model/emitter/* archive/model/emitter/
rmdir src/model/emitter
```

### Step 3: Move Semantic Files

```bash
# Old graph system
mv src/semantic/model_graph/* archive/semantic/model_graph/
rmdir src/semantic/model_graph

# Old support files
mv src/semantic/column_lineage.rs archive/semantic/
mv src/semantic/executor.rs archive/semantic/
mv src/semantic/semantic_model.rs archive/semantic/

# Old planner (entire directory)
mv src/semantic/planner/* archive/semantic/planner/
rmdir src/semantic/planner
```

### Step 4: Move Integration Files

```bash
mv src/translation/mod.rs archive/translation/
rmdir src/translation

mv src/web/server.rs archive/web/
# Keep src/web/ directory if other files exist

mv src/lsp/project.rs archive/lsp/

mv src/metadata/types.rs archive/metadata/
```

### Step 5: Create Archive README

```bash
# Create the README.md as shown above
cat > archive/README.md << 'EOF'
[README content from above]
EOF
```

### Step 6: Update src/model/mod.rs

Remove archived module declarations:

```rust
// Remove these:
// pub mod expr;
// pub mod fact;
// pub mod query;
// pub mod pivot_report;
// pub mod source;
// pub mod dimension_role;
// pub mod loader;
// pub mod emitter;

// Keep these:
pub mod calendar;
pub mod defaults;
pub mod dimension;
pub mod measure;
pub mod report;
pub mod table;
pub mod types;
```

### Step 7: Update src/semantic/mod.rs

Remove archived module declarations:

```rust
// Remove these:
// pub mod model_graph;
// pub mod column_lineage;
// pub mod executor;
// pub mod semantic_model;
// pub mod planner;

// Keep these:
pub mod graph;      // UnifiedGraph
pub mod inference;  // Inference engine
pub mod error;      // Error types
```

### Step 8: Update src/lib.rs

Remove archived top-level modules:

```rust
// Remove these:
// pub mod translation;

// Keep/update these:
pub mod cache;
pub mod config;
pub mod crypto;
pub mod dsl;
pub mod lowering;
pub mod lsp;        // Will need updates later
pub mod metadata;   // Will need updates later
pub mod model;
pub mod semantic;
pub mod sql;
```

### Step 9: Verify Compilation

```bash
cargo check 2>&1 | head -20
```

Expected: Far fewer errors (only missing implementations, not missing types)

### Step 10: Commit

```bash
git add archive/
git add src/
git commit -m "refactor: archive old model system and planner

Archive ~30 files from old architecture to ./archive/:
- Old model types (expr, fact, query, pivot_report, source, dimension_role)
- Old loader (Lua-based) and emitters
- Old ModelGraph (entity-level graph)
- Old column_lineage, executor, semantic_model
- Old query planner (complete directory)
- Old translation layer, web server integration
- Old LSP project integration, metadata types

Keeping only new DSL-based system:
- src/dsl/ - Parser (lexer, AST, validation)
- src/lowering/ - DSL → Model translator
- src/model/ - New types (calendar, dimension, table, measure, report)
- src/semantic/graph/ - UnifiedGraph (column-level)
- src/semantic/inference/ - Inference engine
- src/cache/ - Two-tier graph cache

Clean foundation for rebuilding query planner with UnifiedGraph.

See archive/README.md for details on archived code."
```

---

## Post-Cleanup State

### What Compiles

After cleanup, these modules should compile cleanly:
- ✅ `src/dsl/` - DSL parser
- ✅ `src/lowering/` - DSL → Model translator
- ✅ `src/model/` - New model types
- ✅ `src/semantic/graph/` - UnifiedGraph
- ✅ `src/semantic/inference/` - Inference engine
- ✅ `src/cache/` - Graph cache
- ✅ `src/sql/` - SQL generation
- ✅ `src/config/` - Configuration
- ✅ `src/crypto/` - Encryption

### What Needs Rebuilding

These will have errors until rebuilt:
- ❌ `src/metadata/` - Remove old type references
- ❌ `src/lsp/` - Update to use new loader/model
- ⚠️ Other files may have minor issues

### Next Steps After Cleanup

1. **Rebuild Query Planner** - Build new planner using UnifiedGraph
2. **Rebuild Translation Layer** - Translate queries using new planner
3. **Rebuild Executor** - Execute queries with new planner
4. **Update LSP** - Use new model loading
5. **Update Metadata** - Use new type definitions
6. **Rebuild Web Server** - Expose new planner via API

---

## Rollback Plan

If needed, restore from archive:

```bash
# Restore all archived code
cp -r archive/model/* src/model/
cp -r archive/semantic/* src/semantic/
cp -r archive/translation/* src/translation/
cp -r archive/web/* src/web/
cp -r archive/lsp/* src/lsp/
cp -r archive/metadata/* src/metadata/

# Or just restore specific files
cp archive/semantic/planner/emit.rs src/semantic/planner/
```

Or use git:
```bash
git revert <commit-hash>
```

---

## Benefits

1. **Clean compilation** - No more 446 errors from missing types
2. **Clear architecture** - Only new DSL-based code remains
3. **Preserved history** - All old code in archive/ for reference
4. **Fresh start** - Rebuild planner properly with UnifiedGraph
5. **Focused scope** - Clear what needs to be built vs what's done

## Risks

1. **Lost functionality** - Old planner had working query generation
2. **Reference needed** - May need to check archived code during rebuild
3. **Time to rebuild** - New planner is significant work

**Mitigation:** Archive preserves everything, git history available, comprehensive docs exist for new architecture.
