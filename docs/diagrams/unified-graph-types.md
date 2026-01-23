# Unified Graph Type System Architecture

This document describes the type system architecture for the unified semantic graph, which replaces the dual-graph architecture (ModelGraph + ColumnLineageGraph) with a single unified graph.

## Overview

The unified graph contains **four types of nodes** and **five types of edges**, all stored in a single `petgraph::DiGraph<GraphNode, GraphEdge>`.

## Node Types

### Node Type Hierarchy

```mermaid
classDiagram
    class GraphNode {
        <<enum>>
        +Entity(EntityNode)
        +Column(ColumnNode)
        +Measure(MeasureNode)
        +Calendar(CalendarNode)
        +name() String
        +qualified_name() String
    }
    
    class EntityNode {
        +name: String
        +entity_type: EntityType
        +physical_name: Option~String~
        +schema: Option~String~
        +row_count: Option~usize~
        +size_category: SizeCategory
        +metadata: HashMap~String, String~
    }
    
    class ColumnNode {
        +entity: String
        +name: String
        +data_type: DataType
        +nullable: bool
        +unique: bool
        +primary_key: bool
        +metadata: HashMap~String, String~
        +qualified_name() String
    }
    
    class MeasureNode {
        +name: String
        +entity: String
        +aggregation: String
        +source_column: Option~String~
        +expression: Option~String~
        +metadata: HashMap~String, String~
    }
    
    class CalendarNode {
        +name: String
        +physical_name: String
        +schema: Option~String~
        +date_column: String
        +grain_levels: Vec~String~
        +metadata: HashMap~String, String~
    }
    
    GraphNode *-- EntityNode
    GraphNode *-- ColumnNode
    GraphNode *-- MeasureNode
    GraphNode *-- CalendarNode
```

### Supporting Enums for Nodes

```mermaid
classDiagram
    class EntityType {
        <<enum>>
        Source
        Fact
        Dimension
        Calendar
    }
    
    class SizeCategory {
        <<enum>>
        Small
        Medium
        Large
        Unknown
    }
    
    class DataType {
        <<enum>>
        String
        Integer
        Float
        Boolean
        Date
        Timestamp
        Json
        Unknown
    }
    
    EntityNode --> EntityType
    EntityNode --> SizeCategory
    ColumnNode --> DataType
```

## Edge Types

### Edge Type Hierarchy

```mermaid
classDiagram
    class GraphEdge {
        <<enum>>
        +BelongsTo(BelongsToEdge)
        +References(ReferencesEdge)
        +DerivedFrom(DerivedFromEdge)
        +DependsOn(DependsOnEdge)
        +JoinsTo(JoinsToEdge)
        +edge_type() &str
    }
    
    class BelongsToEdge {
        +column: String
        +entity: String
    }
    
    class ReferencesEdge {
        +from_column: String
        +to_column: String
        +source: RelationshipSource
    }
    
    class DerivedFromEdge {
        +target: String
        +sources: Vec~String~
        +expression: Option~String~
    }
    
    class DependsOnEdge {
        +measure: String
        +columns: Vec~String~
    }
    
    class JoinsToEdge {
        +from_entity: String
        +to_entity: String
        +join_columns: Vec~(String, String)~
        +cardinality: Cardinality
        +source: RelationshipSource
    }
    
    GraphEdge *-- BelongsToEdge
    GraphEdge *-- ReferencesEdge
    GraphEdge *-- DerivedFromEdge
    GraphEdge *-- DependsOnEdge
    GraphEdge *-- JoinsToEdge
```

### Supporting Enums for Edges

```mermaid
classDiagram
    class Cardinality {
        <<enum>>
        OneToOne
        OneToMany
        ManyToOne
        ManyToMany
        Unknown
        +reverse() Cardinality
        +from_uniqueness(bool, bool) Cardinality
    }
    
    class RelationshipSource {
        <<enum>>
        Explicit
        ForeignKey
        Convention
        Statistical
    }
    
    JoinsToEdge --> Cardinality
    JoinsToEdge --> RelationshipSource
    ReferencesEdge --> RelationshipSource
```

## Graph Structure

### Complete Graph Schema

```mermaid
graph TB
    subgraph "Node Types"
        E[EntityNode]
        C[ColumnNode]
        M[MeasureNode]
        Cal[CalendarNode]
    end
    
    subgraph "Edge Types"
        BT[BELONGS_TO]
        R[REFERENCES]
        DF[DERIVED_FROM]
        DO[DEPENDS_ON]
        JT[JOINS_TO]
    end
    
    C -->|BELONGS_TO| E
    C -->|REFERENCES| C
    C -->|DERIVED_FROM| C
    M -->|DEPENDS_ON| C
    E -->|JOINS_TO| E
    
    style E fill:#e1f5ff
    style C fill:#fff4e1
    style M fill:#ffe1f5
    style Cal fill:#e1ffe1
```

### Example Graph Instance

```mermaid
graph LR
    subgraph "Entities"
        Orders[Entity: orders]
        Customers[Entity: customers]
    end
    
    subgraph "Columns"
        OID[Column: orders.id]
        OCustID[Column: orders.customer_id]
        OAmt[Column: orders.amount]
        CID[Column: customers.id]
        CName[Column: customers.name]
    end
    
    subgraph "Measures"
        TotalRev[Measure: orders.total_revenue]
    end
    
    OID -->|BELONGS_TO| Orders
    OCustID -->|BELONGS_TO| Orders
    OAmt -->|BELONGS_TO| Orders
    CID -->|BELONGS_TO| Customers
    CName -->|BELONGS_TO| Customers
    
    OCustID -->|REFERENCES| CID
    Orders -->|JOINS_TO| Customers
    
    TotalRev -->|DEPENDS_ON| OAmt
    
    style Orders fill:#e1f5ff
    style Customers fill:#e1f5ff
    style OID fill:#fff4e1
    style OCustID fill:#fff4e1
    style OAmt fill:#fff4e1
    style CID fill:#fff4e1
    style CName fill:#fff4e1
    style TotalRev fill:#ffe1f5
```

## UnifiedGraph Structure

```mermaid
classDiagram
    class UnifiedGraph {
        -graph: DiGraph~GraphNode, GraphEdge~
        -node_index: HashMap~String, NodeIndex~
        -entity_index: HashMap~String, NodeIndex~
        -column_index: HashMap~String, NodeIndex~
        -measure_index: HashMap~String, NodeIndex~
        -calendar_index: HashMap~String, NodeIndex~
        +new() UnifiedGraph
    }
    
    UnifiedGraph *-- GraphNode
    UnifiedGraph *-- GraphEdge
    UnifiedGraph --> "uses" NodeIndex
```

## Design Principles

### 1. Single Source of Truth
- All semantic elements (entities, columns, measures, calendars) are nodes in one graph
- No separate graphs for different concerns
- Relationships are explicit edges with typed metadata

### 2. Typed Edges
Each edge type has a specific purpose:
- **BELONGS_TO**: Structural ownership (column → entity)
- **REFERENCES**: Foreign key relationships (column → column)
- **DERIVED_FROM**: Column lineage (column → column(s))
- **DEPENDS_ON**: Measure dependencies (measure → column(s))
- **JOINS_TO**: Table-level joins (entity → entity)

### 3. Rich Metadata
- Every node and edge can carry custom metadata
- Provenance tracking via `RelationshipSource`
- Size categories for query optimization
- Data types for validation

### 4. Indexing Strategy
Multiple indices for efficient lookups:
- `node_index`: Universal name → NodeIndex
- `entity_index`: Entity-specific lookups
- `column_index`: Qualified column names (entity.column)
- `measure_index`: Qualified measure names (entity.measure)
- `calendar_index`: Calendar-specific lookups

## Migration from Dual-Graph Architecture

### Old Architecture
```
ModelGraph (entities + relationships)
      +
ColumnLineageGraph (columns + transformations)
```

### New Architecture
```
UnifiedGraph (entities + columns + measures + calendars + all relationships)
```

### Benefits
1. **Simpler mental model**: One graph, not two
2. **Easier queries**: No cross-graph coordination
3. **Better lineage**: Full path from measure → column → entity
4. **Extensible**: Add new node/edge types without creating new graphs

## Type Safety

All node and edge types are strongly typed Rust enums:
- Compile-time guarantees about graph structure
- Pattern matching for exhaustive edge handling
- No runtime type confusion
- Clear API surface

## Next Steps

1. **Task 2**: Implement builder methods to populate the graph
2. **Task 3**: Add graph query methods (find_path, get_lineage, etc.)
3. **Task 4**: Implement conversion from DSL Model → UnifiedGraph
4. **Task 5**: Add serialization/deserialization support
