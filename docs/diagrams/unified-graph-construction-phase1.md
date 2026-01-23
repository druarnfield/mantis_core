# Unified Graph Construction - Phase 1: Node Creation

This document visualizes the Phase 1 implementation of the unified semantic graph construction, which creates all nodes (entities, columns, measures, calendars) before Phase 2 creates the edges between them.

## Overview

Phase 1 creates four types of nodes from the DSL model:
1. **Entity Nodes** - from tables and dimensions
2. **Column Nodes** - from atoms and attributes (with BELONGS_TO edges)
3. **Measure Nodes** - from measure blocks
4. **Calendar Nodes** - from calendar definitions

## Construction Flow

```mermaid
graph TB
    subgraph Input["Input Sources"]
        Model["DSL Model<br/>(tables, dimensions,<br/>measures, calendars)"]
        Stats["Column Statistics<br/>(row counts,<br/>cardinality)"]
        Rels["Inferred Relationships<br/>(from inference engine)"]
    end
    
    subgraph Phase1["Phase 1: Node Creation"]
        EntNodes["create_entity_nodes()<br/>Tables → Fact entities<br/>Dimensions → Dimension entities"]
        ColNodes["create_column_nodes()<br/>Atoms → ColumnNode<br/>Attributes → ColumnNode<br/>+ BELONGS_TO edges"]
        MeasNodes["create_measure_nodes()<br/>Measures → MeasureNode"]
        CalNodes["create_calendar_nodes()<br/>Calendars → CalendarNode"]
    end
    
    subgraph Phase2["Phase 2: Edge Creation (Task 3)"]
        RefEdges["create_references_edges()<br/>(FK relationships)"]
        JoinEdges["create_joins_to_edges()<br/>(entity-level joins)"]
        DepEdges["create_depends_on_edges()<br/>(measure dependencies)"]
    end
    
    subgraph Output["Output"]
        Graph["UnifiedGraph<br/>(all nodes + edges)"]
    end
    
    Model --> EntNodes
    Model --> ColNodes
    Model --> MeasNodes
    Model --> CalNodes
    
    Stats --> ColNodes
    
    EntNodes --> ColNodes
    ColNodes --> MeasNodes
    MeasNodes --> CalNodes
    
    CalNodes --> RefEdges
    Rels --> RefEdges
    Rels --> JoinEdges
    Model --> DepEdges
    
    RefEdges --> JoinEdges
    JoinEdges --> DepEdges
    DepEdges --> Graph
    
    style Phase1 fill:#e1f5e1
    style Phase2 fill:#fff4e1
    style Input fill:#e1f0ff
    style Output fill:#ffe1f0
```

## Entity Node Creation

```mermaid
graph LR
    subgraph DSL["DSL Model"]
        Table1["table sales { ... }"]
        Dim1["dimension customers { ... }"]
    end
    
    subgraph Graph["UnifiedGraph"]
        EntNode1["EntityNode<br/>name: sales<br/>type: Fact<br/>physical: dbo.fact_sales"]
        EntNode2["EntityNode<br/>name: customers<br/>type: Dimension<br/>physical: dbo.dim_customers"]
    end
    
    Table1 -->|create_entity_nodes| EntNode1
    Dim1 -->|create_entity_nodes| EntNode2
    
    style DSL fill:#e1f0ff
    style Graph fill:#ffe1f0
```

## Column Node Creation with Statistics

```mermaid
graph TB
    subgraph DSL["DSL Model"]
        Table["table sales {<br/>  atoms {<br/>    quantity int;<br/>    amount decimal;<br/>  }<br/>}"]
    end
    
    subgraph Stats["Column Statistics"]
        Stat1["quantity:<br/>total: 50,000<br/>distinct: 1,000<br/>unique: false"]
        Stat2["amount:<br/>total: 50,000<br/>distinct: 25,000<br/>unique: false"]
    end
    
    subgraph Graph["UnifiedGraph"]
        EntNode["EntityNode<br/>name: sales<br/>row_count: 50,000<br/>size: Small"]
        ColNode1["ColumnNode<br/>entity: sales<br/>name: quantity<br/>type: Integer<br/>unique: false"]
        ColNode2["ColumnNode<br/>entity: sales<br/>name: amount<br/>type: Float<br/>unique: false"]
        
        ColNode1 -.->|BELONGS_TO| EntNode
        ColNode2 -.->|BELONGS_TO| EntNode
    end
    
    Table -->|create_column_nodes| ColNode1
    Table -->|create_column_nodes| ColNode2
    Stat1 -.->|enrich| ColNode1
    Stat2 -.->|enrich| ColNode2
    Stat1 -.->|set row_count| EntNode
    
    style DSL fill:#e1f0ff
    style Stats fill:#fff4e1
    style Graph fill:#ffe1f0
```

## Size Category Assignment

```mermaid
graph LR
    subgraph Stats["Row Count from Statistics"]
        Small["< 100K rows<br/>(dimensions)"]
        Medium["100K - 10M rows<br/>(medium facts)"]
        Large["> 10M rows<br/>(large facts)"]
    end
    
    subgraph Categories["Size Categories"]
        Cat1["SizeCategory::Small"]
        Cat2["SizeCategory::Medium"]
        Cat3["SizeCategory::Large"]
    end
    
    Small --> Cat1
    Medium --> Cat2
    Large --> Cat3
    
    Cat1 -.->|used for| Opt1["Join optimization<br/>(broadcast small tables)"]
    Cat2 -.->|used for| Opt2["Query planning<br/>(partition strategy)"]
    Cat3 -.->|used for| Opt3["Cardinality estimation<br/>(join ordering)"]
    
    style Stats fill:#fff4e1
    style Categories fill:#e1f5e1
```

## Dimension Attribute Nodes

```mermaid
graph TB
    subgraph DSL["DSL Model"]
        Dim["dimension customers {<br/>  key: customer_id;<br/>  attributes {<br/>    name string;<br/>    email string;<br/>  }<br/>}"]
    end
    
    subgraph Graph["UnifiedGraph"]
        EntNode["EntityNode<br/>name: customers<br/>type: Dimension"]
        
        KeyNode["ColumnNode<br/>entity: customers<br/>name: customer_id<br/>primary_key: true<br/>unique: true<br/>nullable: false"]
        
        AttrNode1["ColumnNode<br/>entity: customers<br/>name: name<br/>type: String<br/>primary_key: false"]
        
        AttrNode2["ColumnNode<br/>entity: customers<br/>name: email<br/>type: String<br/>primary_key: false"]
        
        KeyNode -.->|BELONGS_TO| EntNode
        AttrNode1 -.->|BELONGS_TO| EntNode
        AttrNode2 -.->|BELONGS_TO| EntNode
    end
    
    Dim -->|create_column_nodes| KeyNode
    Dim -->|create_column_nodes| AttrNode1
    Dim -->|create_column_nodes| AttrNode2
    
    style DSL fill:#e1f0ff
    style Graph fill:#ffe1f0
```

## Measure Node Creation

```mermaid
graph LR
    subgraph DSL["DSL Model"]
        Measures["measures sales {<br/>  total_quantity = { SUM(quantity) };<br/>  total_amount = { SUM(amount) };<br/>  avg_price = { SUM(amount) / SUM(quantity) };<br/>}"]
    end
    
    subgraph Graph["UnifiedGraph"]
        M1["MeasureNode<br/>name: total_quantity<br/>entity: sales<br/>aggregation: CUSTOM<br/>expr: SUM(quantity)"]
        
        M2["MeasureNode<br/>name: total_amount<br/>entity: sales<br/>aggregation: CUSTOM<br/>expr: SUM(amount)"]
        
        M3["MeasureNode<br/>name: avg_price<br/>entity: sales<br/>aggregation: CUSTOM<br/>expr: SUM(amount) / SUM(quantity)"]
    end
    
    Measures -->|create_measure_nodes| M1
    Measures -->|create_measure_nodes| M2
    Measures -->|create_measure_nodes| M3
    
    style DSL fill:#e1f0ff
    style Graph fill:#ffe1f0
```

## Calendar Node Creation

```mermaid
graph TB
    subgraph DSL["DSL Model"]
        PhysCal["calendar dates {<br/>  physical dbo.dim_date;<br/>  grain {<br/>    day: date_key;<br/>    month: month_start;<br/>  }<br/>}"]
        
        GenCal["calendar fiscal {<br/>  generated day+;<br/>  fiscal_year_start january;<br/>}"]
    end
    
    subgraph Graph["UnifiedGraph"]
        CalNode1["CalendarNode<br/>name: dates<br/>physical: dbo.dim_date<br/>date_column: date_key<br/>grain_levels: [day, month]"]
        
        CalNode2["CalendarNode<br/>name: fiscal<br/>physical: generated_fiscal<br/>date_column: date<br/>grain_levels: [day, week,<br/>month, quarter, year]"]
    end
    
    PhysCal -->|create_calendar_nodes| CalNode1
    GenCal -->|create_calendar_nodes| CalNode2
    
    style DSL fill:#e1f0ff
    style Graph fill:#ffe1f0
```

## Graph Indexes

The UnifiedGraph maintains multiple indexes for fast lookups:

```mermaid
graph TB
    subgraph Graph["UnifiedGraph"]
        PGraph["petgraph::DiGraph<br/>(nodes + edges)"]
        
        subgraph Indexes["Lookup Indexes"]
            NodeIdx["node_index<br/>String → NodeIndex<br/>(all nodes by name)"]
            EntIdx["entity_index<br/>String → NodeIndex<br/>(entities by name)"]
            ColIdx["column_index<br/>String → NodeIndex<br/>(qualified: entity.column)"]
            MeasIdx["measure_index<br/>String → NodeIndex<br/>(qualified: entity.measure)"]
            CalIdx["calendar_index<br/>String → NodeIndex<br/>(calendars by name)"]
        end
    end
    
    PGraph -.->|references| NodeIdx
    PGraph -.->|references| EntIdx
    PGraph -.->|references| ColIdx
    PGraph -.->|references| MeasIdx
    PGraph -.->|references| CalIdx
    
    NodeIdx -.->|O(1) lookup| L1["Any node by name"]
    EntIdx -.->|O(1) lookup| L2["Entity by table/dim name"]
    ColIdx -.->|O(1) lookup| L3["Column by entity.column"]
    MeasIdx -.->|O(1) lookup| L4["Measure by entity.measure"]
    CalIdx -.->|O(1) lookup| L5["Calendar by name"]
    
    style Graph fill:#ffe1f0
    style Indexes fill:#e1f5e1
```

## Complete Example

Here's a complete example showing all node types created from a sample DSL model:

```mermaid
graph TB
    subgraph DSL["Sample DSL Model"]
        T1["table sales {<br/>  atoms { quantity int; }<br/>}"]
        D1["dimension customers {<br/>  key: customer_id;<br/>  attributes { name string; }<br/>}"]
        M1["measures sales {<br/>  total = { SUM(quantity) };<br/>}"]
        C1["calendar dates {<br/>  generated day+;<br/>}"]
    end
    
    subgraph Graph["UnifiedGraph After Phase 1"]
        subgraph Entities["Entity Nodes"]
            E1["sales<br/>(Fact)"]
            E2["customers<br/>(Dimension)"]
        end
        
        subgraph Columns["Column Nodes"]
            Col1["sales.quantity"]
            Col2["customers.customer_id<br/>(PK)"]
            Col3["customers.name"]
        end
        
        subgraph Measures["Measure Nodes"]
            Meas1["sales.total"]
        end
        
        subgraph Calendars["Calendar Nodes"]
            Cal1["dates"]
        end
        
        Col1 -.->|BELONGS_TO| E1
        Col2 -.->|BELONGS_TO| E2
        Col3 -.->|BELONGS_TO| E2
    end
    
    T1 --> E1
    T1 --> Col1
    D1 --> E2
    D1 --> Col2
    D1 --> Col3
    M1 --> Meas1
    C1 --> Cal1
    
    style DSL fill:#e1f0ff
    style Graph fill:#ffe1f0
    style Entities fill:#ffe1e1
    style Columns fill:#e1ffe1
    style Measures fill:#ffe1ff
    style Calendars fill:#e1ffff
```

## Node Type Distribution

```mermaid
pie title "Node Types in Typical Model"
    "Entity Nodes" : 15
    "Column Nodes" : 150
    "Measure Nodes" : 30
    "Calendar Nodes" : 2
```

## Implementation Details

### Error Handling

```mermaid
graph LR
    subgraph Validations["Graph Build Validations"]
        V1["Duplicate entity check"]
        V2["Duplicate column check"]
        V3["Duplicate measure check"]
        V4["Duplicate calendar check"]
        V5["Entity exists check"]
    end
    
    subgraph Errors["GraphBuildError"]
        E1["DuplicateEntity"]
        E2["DuplicateColumn"]
        E3["DuplicateMeasure"]
        E4["DuplicateCalendar"]
        E5["EntityNotFound"]
    end
    
    V1 -->|fails| E1
    V2 -->|fails| E2
    V3 -->|fails| E3
    V4 -->|fails| E4
    V5 -->|fails| E5
    
    style Validations fill:#e1f5e1
    style Errors fill:#ffe1e1
```

### Data Type Mapping

```mermaid
graph LR
    subgraph DSL["DSL AtomType & DataType"]
        AT1["AtomType::Int"]
        AT2["AtomType::Decimal"]
        AT3["AtomType::Float"]
        DT1["DataType::String"]
        DT2["DataType::Date"]
        DT3["DataType::Timestamp"]
    end
    
    subgraph Graph["Graph DataType"]
        GT1["DataType::Integer"]
        GT2["DataType::Float"]
        GT3["DataType::Float"]
        GT4["DataType::String"]
        GT5["DataType::Date"]
        GT6["DataType::Timestamp"]
    end
    
    AT1 --> GT1
    AT2 --> GT2
    AT3 --> GT3
    DT1 --> GT4
    DT2 --> GT5
    DT3 --> GT6
    
    style DSL fill:#e1f0ff
    style Graph fill:#ffe1f0
```

## Next Steps: Phase 2

Phase 2 (Task 3) will implement edge creation:

1. **REFERENCES edges** - FK column → PK column relationships
2. **JOINS_TO edges** - Entity-to-entity join relationships
3. **DEPENDS_ON edges** - Measure → Column dependencies

```mermaid
graph TB
    Phase1["Phase 1 Complete<br/>(This Task)"]
    
    subgraph Phase2["Phase 2: Edge Creation (Task 3)"]
        S1["Implement create_references_edges()<br/>Parse inferred FK relationships<br/>Create REFERENCES edges"]
        S2["Implement create_joins_to_edges()<br/>Aggregate column refs to entity joins<br/>Create JOINS_TO edges with cardinality"]
        S3["Implement create_depends_on_edges()<br/>Parse SQL expressions for column refs<br/>Create DEPENDS_ON edges"]
    end
    
    Phase1 --> S1
    S1 --> S2
    S2 --> S3
    
    style Phase1 fill:#e1f5e1
    style Phase2 fill:#fff4e1
```
