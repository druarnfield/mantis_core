# Unified Graph Query Interface

This document describes the query interface for the unified semantic graph, which provides methods to traverse the graph and answer questions about entity relationships, join paths, and data grain.

## Overview

The query interface implements three main capabilities:

1. **Path Finding** (`find_path`): Find the shortest join path between two entities using BFS
2. **Path Validation** (`validate_safe_path`): Check if a join path is safe (no dangerous fan-out)
3. **Grain Inference** (`infer_grain`): Determine the most granular entity in a set

## Architecture

```mermaid
graph TD
    A[UnifiedGraph] --> B[find_path]
    A --> C[validate_safe_path]
    A --> D[infer_grain]
    
    B --> E[reconstruct_join_path]
    C --> B
    
    B --> F[JoinPath]
    F --> G[JoinStep]
    
    H[QueryError] -.error.-> B
    H -.error.-> C
    H -.error.-> D
    
    style A fill:#e1f5ff
    style B fill:#fff4e1
    style C fill:#fff4e1
    style D fill:#fff4e1
    style F fill:#e8f5e9
    style G fill:#e8f5e9
    style H fill:#ffebee
```

## 1. BFS Path Finding Algorithm

The `find_path` method uses Breadth-First Search (BFS) to find the shortest join path between two entities.

```mermaid
graph TD
    Start([Start: find_path from, to]) --> Init[Initialize: Queue, Visited, Parent Map]
    Init --> Lookup[Look up from_idx and to_idx]
    Lookup --> Check1{Entities exist?}
    Check1 -->|No| Error1[Return EntityNotFound]
    Check1 -->|Yes| BFS[Start BFS]
    
    BFS --> Enqueue[Enqueue source entity]
    Enqueue --> MarkVisit[Mark source as visited]
    MarkVisit --> Loop{Queue empty?}
    
    Loop -->|Yes| Error2[Return NoPathFound]
    Loop -->|No| Dequeue[Dequeue current entity]
    
    Dequeue --> CheckTarget{current == target?}
    CheckTarget -->|Yes| Reconstruct[Call reconstruct_join_path]
    CheckTarget -->|No| Explore[Explore JOINS_TO edges]
    
    Explore --> ForEach{For each neighbor}
    ForEach --> CheckVisited{Visited?}
    CheckVisited -->|Yes| ForEach
    CheckVisited -->|No| MarkNeighbor[Mark visited, Set parent, Enqueue]
    MarkNeighbor --> ForEach
    ForEach --> Loop
    
    Reconstruct --> Return[Return JoinPath]
    
    style Start fill:#e1f5ff
    style Error1 fill:#ffebee
    style Error2 fill:#ffebee
    style Return fill:#e8f5e9
    style BFS fill:#fff4e1
    style Reconstruct fill:#fff4e1
```

### BFS Implementation Details

```rust
// Pseudocode for find_path
fn find_path(from: &str, to: &str) -> Result<JoinPath> {
    // 1. Look up entity indices
    let from_idx = entity_index.get(from)?;
    let to_idx = entity_index.get(to)?;
    
    // 2. Initialize BFS data structures
    let mut queue = VecDeque::new();
    let mut visited = HashSet::new();
    let mut parent = HashMap::new();
    
    queue.push_back(from_idx);
    visited.insert(from_idx);
    
    // 3. BFS traversal
    while let Some(current) = queue.pop_front() {
        if current == to_idx {
            return reconstruct_join_path(from_idx, to_idx, &parent);
        }
        
        // Explore neighbors via JOINS_TO edges
        for edge in graph.edges(current) {
            if edge is JOINS_TO {
                let neighbor = edge.target();
                if !visited.contains(neighbor) {
                    visited.insert(neighbor);
                    parent.insert(neighbor, current);
                    queue.push_back(neighbor);
                }
            }
        }
    }
    
    Err(NoPathFound)
}
```

## 2. Path Reconstruction

The `reconstruct_join_path` helper walks backward from the target to the source using the parent map.

```mermaid
graph TD
    Start([Start: reconstruct_join_path]) --> Init[current = to_idx, steps = empty]
    Init --> Loop{current == from_idx?}
    
    Loop -->|Yes| Reverse[Reverse steps array]
    Loop -->|No| GetParent[Get parent of current]
    
    GetParent --> FindEdge[Find edge from parent to current]
    FindEdge --> Extract[Extract entity names and cardinality]
    Extract --> CreateStep[Create JoinStep]
    CreateStep --> AddStep[Add step to array]
    AddStep --> UpdateCurrent[current = parent]
    UpdateCurrent --> Loop
    
    Reverse --> Return[Return JoinPath]
    
    style Start fill:#e1f5ff
    style Return fill:#e8f5e9
    style CreateStep fill:#fff4e1
```

### Path Reconstruction Example

For a query `find_path("sales", "regions")` through `customers`:

```mermaid
graph LR
    subgraph "Forward BFS"
        S[sales] -->|parent| C[customers]
        C -->|parent| R[regions]
    end
    
    subgraph "Backward Reconstruction"
        R2[regions] -.current.-> C2[customers]
        C2 -.current.-> S2[sales]
    end
    
    subgraph "Result JoinPath"
        S3[sales] -->|Step 1: N:1| C3[customers]
        C3 -->|Step 2: N:1| R3[regions]
    end
    
    style S fill:#e1f5ff
    style C fill:#e1f5ff
    style R fill:#e1f5ff
    style S3 fill:#e8f5e9
    style C3 fill:#e8f5e9
    style R3 fill:#e8f5e9
```

## 3. Path Validation (Safe Join Check)

The `validate_safe_path` method checks if a join path contains dangerous fan-out (OneToMany joins).

```mermaid
graph TD
    Start([Start: validate_safe_path from, to]) --> FindPath[Call find_path]
    FindPath --> CheckError{Path found?}
    CheckError -->|No| Error[Return NoPathFound]
    CheckError -->|Yes| Loop{For each step in path}
    
    Loop -->|Done| Success[Return Ok]
    Loop -->|Next step| CheckCard{Cardinality contains 'OneToMany'?}
    
    CheckCard -->|Yes| Error2[Return UnsafeJoinPath]
    CheckCard -->|No| Loop
    
    style Start fill:#e1f5ff
    style Error fill:#ffebee
    style Error2 fill:#ffebee
    style Success fill:#e8f5e9
```

### Fan-Out Detection

```mermaid
graph TD
    subgraph "Safe Path: ManyToOne"
        S1[sales: 1M rows] -->|N:1| C1[customers: 10K rows]
        C1 -->|N:1| R1[regions: 100 rows]
        style S1 fill:#e8f5e9
        style C1 fill:#e8f5e9
        style R1 fill:#e8f5e9
    end
    
    subgraph "Unsafe Path: OneToMany"
        C2[customers: 10K rows] -->|1:N| O2[orders: 100K rows]
        Note2[May cause row duplication!]
        style C2 fill:#ffebee
        style O2 fill:#ffebee
        style Note2 fill:#fff4e1
    end
```

## 4. Grain Inference

The `infer_grain` method finds the entity with the highest row count (most granular).

```mermaid
graph TD
    Start([Start: infer_grain entities]) --> Init[max_rows = 0, grain = None]
    Init --> Loop{For each entity}
    
    Loop -->|Done| CheckGrain{grain found?}
    Loop -->|Next| Lookup[Look up entity_idx]
    
    Lookup --> CheckExists{Entity exists?}
    CheckExists -->|No| Error[Return EntityNotFound]
    CheckExists -->|Yes| GetNode[Get entity node]
    
    GetNode --> CheckRowCount{Has row_count?}
    CheckRowCount -->|No| Loop
    CheckRowCount -->|Yes| Compare{rows > max_rows?}
    
    Compare -->|No| Loop
    Compare -->|Yes| Update[Update max_rows and grain]
    Update --> Loop
    
    CheckGrain -->|No| Error2[Return EntityNotFound]
    CheckGrain -->|Yes| Return[Return grain entity name]
    
    style Start fill:#e1f5ff
    style Error fill:#ffebee
    style Error2 fill:#ffebee
    style Return fill:#e8f5e9
```

### Grain Inference Example

```mermaid
graph TD
    subgraph "Entity Row Counts"
        R[regions: 100 rows] 
        C[customers: 10,000 rows]
        O[orders: 100,000 rows]
        I[order_items: 500,000 rows]
    end
    
    subgraph "Grain Inference Result"
        Result[Grain = order_items<br/>Highest row count: 500,000]
    end
    
    R --> Result
    C --> Result
    O --> Result
    I --> Result
    
    style I fill:#e8f5e9
    style Result fill:#fff4e1
```

## Example Usage

### Example 1: Finding a Join Path

```rust
use crate::semantic::graph::UnifiedGraph;

let graph = UnifiedGraph::from_model_with_inference(&model, &relationships, &stats)?;

// Find path from sales to regions (through customers)
let path = graph.find_path("sales", "regions")?;

// Path has 2 steps:
// Step 1: sales -> customers (N:1)
// Step 2: customers -> regions (N:1)
assert_eq!(path.steps.len(), 2);
assert_eq!(path.steps[0].from, "sales");
assert_eq!(path.steps[0].to, "customers");
assert_eq!(path.steps[0].cardinality, "N:1");
```

```mermaid
sequenceDiagram
    participant Client
    participant UnifiedGraph
    participant BFS as BFS Algorithm
    participant Reconstruct as reconstruct_join_path
    
    Client->>UnifiedGraph: find_path("sales", "regions")
    UnifiedGraph->>BFS: Start BFS from sales
    BFS->>BFS: Visit sales
    BFS->>BFS: Visit customers (parent: sales)
    BFS->>BFS: Visit regions (parent: customers)
    BFS->>BFS: Found target!
    BFS->>Reconstruct: parent_map
    Reconstruct->>Reconstruct: Walk backward: regions -> customers -> sales
    Reconstruct->>Reconstruct: Create steps and reverse
    Reconstruct-->>Client: JoinPath { steps: [sales->customers, customers->regions] }
```

### Example 2: Validating a Safe Path

```rust
// Validate safe path (ManyToOne joins)
graph.validate_safe_path("sales", "customers")?; // OK
graph.validate_safe_path("sales", "regions")?;   // OK

// Unsafe path (OneToMany join)
let result = graph.validate_safe_path("customers", "orders");
assert!(result.is_err()); // Error: OneToMany fan-out
```

```mermaid
sequenceDiagram
    participant Client
    participant UnifiedGraph
    participant FindPath as find_path
    participant Validator as Path Validator
    
    Client->>UnifiedGraph: validate_safe_path("customers", "orders")
    UnifiedGraph->>FindPath: find_path("customers", "orders")
    FindPath-->>UnifiedGraph: JoinPath { steps: [customers->orders(1:N)] }
    UnifiedGraph->>Validator: Check each step
    Validator->>Validator: Step 1: customers->orders (1:N)
    Validator->>Validator: Contains "OneToMany"!
    Validator-->>Client: Error: UnsafeJoinPath { reason: "May cause row duplication" }
```

### Example 3: Inferring Grain

```rust
// Infer grain from multiple entities
let entities = &["sales", "customers", "products", "regions"];
let grain = graph.infer_grain(entities)?;

// sales has highest row count (most granular)
assert_eq!(grain, "sales");
```

```mermaid
sequenceDiagram
    participant Client
    participant UnifiedGraph
    participant Iterator as Entity Iterator
    
    Client->>UnifiedGraph: infer_grain(["sales", "customers", "products", "regions"])
    UnifiedGraph->>Iterator: For each entity
    Iterator->>UnifiedGraph: Check sales: 1,000,000 rows
    Iterator->>UnifiedGraph: Check customers: 10,000 rows
    Iterator->>UnifiedGraph: Check products: 5,000 rows
    Iterator->>UnifiedGraph: Check regions: 100 rows
    UnifiedGraph->>UnifiedGraph: Max rows: sales (1,000,000)
    UnifiedGraph-->>Client: "sales"
```

## Data Structures

### JoinPath

A complete join path between two entities.

```rust
pub struct JoinPath {
    pub steps: Vec<JoinStep>,
}
```

### JoinStep

A single step in a join path.

```rust
pub struct JoinStep {
    pub from: String,        // Source entity
    pub to: String,          // Target entity
    pub cardinality: String, // "1:1", "1:N", "N:1", "N:N"
}
```

### QueryError

Errors that can occur during queries.

```rust
pub enum QueryError {
    EntityNotFound(String),
    ColumnNotFound(String),
    MeasureNotFound(String),
    CalendarNotFound(String),
    NoPathFound { from: String, to: String },
    UnsafeJoinPath { from: String, to: String, reason: String },
}
```

## Graph Traversal Patterns

```mermaid
graph TD
    subgraph "JOINS_TO Edge Network"
        S[sales] -->|N:1| C[customers]
        S -->|N:1| P[products]
        S -->|N:1| D[dates]
        C -->|N:1| R[regions]
        P -->|N:1| Cat[categories]
    end
    
    subgraph "Possible Paths"
        Path1[sales -> customers -> regions]
        Path2[sales -> products -> categories]
        Path3[sales -> dates]
    end
    
    S -.path1.-> Path1
    S -.path2.-> Path2
    S -.path3.-> Path3
    
    style S fill:#e1f5ff
    style Path1 fill:#e8f5e9
    style Path2 fill:#e8f5e9
    style Path3 fill:#e8f5e9
```

## Performance Characteristics

| Operation | Time Complexity | Space Complexity | Notes |
|-----------|----------------|------------------|-------|
| `find_path` | O(V + E) | O(V) | V = entities, E = relationships |
| `reconstruct_join_path` | O(L) | O(L) | L = path length |
| `validate_safe_path` | O(V + E + L) | O(V + L) | Includes find_path |
| `infer_grain` | O(N) | O(1) | N = number of entities |

## Error Handling

```mermaid
graph TD
    Query[Query Method] --> Check1{Entity exists?}
    Check1 -->|No| E1[EntityNotFound]
    Check1 -->|Yes| Check2{Path exists?}
    Check2 -->|No| E2[NoPathFound]
    Check2 -->|Yes| Check3{Safe path?}
    Check3 -->|No| E3[UnsafeJoinPath]
    Check3 -->|Yes| Success[Return Result]
    
    style E1 fill:#ffebee
    style E2 fill:#ffebee
    style E3 fill:#ffebee
    style Success fill:#e8f5e9
```

## Future Extensions

The query interface will be extended with:

1. **Column-level queries**: Trace lineage through DERIVED_FROM edges
2. **Measure queries**: Resolve measure dependencies via DEPENDS_ON edges
3. **Calendar queries**: Find time dimensions and grain levels
4. **Multi-path queries**: Find all paths (not just shortest)
5. **Cost-based routing**: Choose paths based on cardinality and row counts
6. **Circular dependency detection**: Identify cycles in derivation chains

## Related Documentation

- [Unified Graph Architecture](./unified-graph-architecture.md)
- [Graph Construction Phase 1](./unified-graph-construction-phase1.md)
- [Type System](../../src/semantic/graph/types.rs)
- [Builder Implementation](../../src/semantic/graph/builder.rs)
