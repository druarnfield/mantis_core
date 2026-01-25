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
2. Check git history for the archival commit
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

## Rollback

If needed, restore from archive:
```bash
cp -r archive/model/* src/model/
cp -r archive/semantic/* src/semantic/
# etc.
```

Or use git to revert the archival commit.
