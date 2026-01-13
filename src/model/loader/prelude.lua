-- =============================================================================
-- Mantis Prelude
-- =============================================================================
-- Automatically loaded before user models to provide a cleaner DSL.
-- All definitions here can be overridden by user code if needed.

-- Save reference to Lua's built-in table library (may have been saved by Rust as _lua_table)
local lua_table = _lua_table or table

-- =============================================================================
-- Type Constants
-- =============================================================================

-- Integer types
int8 = "int8"
int16 = "int16"
int32 = "int32"
int64 = "int64"
tinyint = "int8"
smallint = "int16"
int = "int32"
integer = "int32"
bigint = "int64"

-- Float types
float32 = "float32"
float64 = "float64"
float = "float32"
double = "float64"
real = "float32"

-- String types
string = "string"
text = "string"

-- Date/Time types
date = "date"
time = "time"
timestamp = "timestamp"
datetime = "timestamp"
timestamptz = "timestamptz"
datetimeoffset = "timestamptz"

-- Other types
bool = "bool"
boolean = "bool"
json = "json"
jsonb = "json"
uuid = "uuid"
binary = "binary"
blob = "binary"

-- =============================================================================
-- Parameterized Type Constructors
-- =============================================================================

function decimal(precision, scale)
    scale = scale or 0
    return string.format("decimal(%d,%d)", precision, scale)
end

function numeric(precision, scale)
    return decimal(precision, scale)
end

function varchar(len)
    return string.format("varchar(%d)", len)
end

function char(len)
    return string.format("char(%d)", len)
end

function nvarchar(len)
    return string.format("varchar(%d)", len)  -- Normalized to varchar
end

-- =============================================================================
-- Column Modifiers
-- =============================================================================

--- Mark a column as primary key
-- @param type_str The data type string
-- @return Column definition table
function pk(type_str)
    return { type = type_str, primary_key = true, nullable = false }
end

--- Mark a column as non-nullable (required)
-- @param type_str The data type string
-- @return Column definition table
function required(type_str)
    return { type = type_str, nullable = false }
end

--- Mark a column as nullable (explicit)
-- @param type_str The data type string
-- @return Column definition table
function nullable(type_str)
    return { type = type_str, nullable = true }
end

--- Create a column with full options
-- @param type_str The data type string
-- @param opts Optional table with nullable, description, etc.
-- @return Column definition table
function column(type_str, opts)
    opts = opts or {}
    opts.type = type_str
    return opts
end

--- Add a description to a column definition
-- @param col_def Column definition table
-- @param desc Description string
-- @return Modified column definition
function describe(col_def, desc)
    if type(col_def) == "string" then
        col_def = { type = col_def }
    end
    col_def.description = desc
    return col_def
end

-- =============================================================================
-- Measure Constructors
-- =============================================================================
-- Measures support method chaining with :where() for filtered aggregations:
--   revenue = sum "total"
--   completed_revenue = sum("total"):where("status = 'completed'")

-- Metatable for measures that enables :where() chaining
local measure_mt = {
    __index = {
        --- Add a filter condition to a measure
        -- @param condition SQL filter condition
        -- @return The modified measure (for chaining)
        where = function(self, condition)
            self.filter = condition
            return self
        end
    }
}

--- Create a SUM measure
-- @param col Column name to sum
-- @return Measure definition table with :where() method
function sum(col)
    return setmetatable({ agg = "sum", column = col }, measure_mt)
end

--- Create a COUNT measure
-- @param col Column name to count (optional, defaults to "*")
-- @return Measure definition table with :where() method
function count(col)
    return setmetatable({ agg = "count", column = col or "*" }, measure_mt)
end

--- Create a COUNT DISTINCT measure
-- @param col Column name to count distinct values
-- @return Measure definition table with :where() method
function count_distinct(col)
    return setmetatable({ agg = "count_distinct", column = col }, measure_mt)
end

--- Create an AVG measure
-- @param col Column name to average
-- @return Measure definition table with :where() method
function avg(col)
    return setmetatable({ agg = "avg", column = col }, measure_mt)
end

--- Create a MIN measure
-- @param col Column name
-- @return Measure definition table with :where() method
function min(col)
    return setmetatable({ agg = "min", column = col }, measure_mt)
end

--- Create a MAX measure
-- @param col Column name
-- @return Measure definition table with :where() method
function max(col)
    return setmetatable({ agg = "max", column = col }, measure_mt)
end

--- Add a filter condition to a measure (legacy function style)
-- @param condition SQL WHERE clause condition
-- @param measure_def Measure definition to filter (can be table or string column name)
-- @return Modified measure definition
function when(condition, measure_def)
    -- Handle case where measure_def is a string (column name for sum/count)
    if type(measure_def) == "string" then
        -- Assume it's a column name for a sum aggregation (most common case)
        measure_def = setmetatable({ agg = "sum", column = measure_def }, measure_mt)
    end
    measure_def.filter = condition
    return measure_def
end

--- Add a filter condition to a measure (alias)
function filtered(measure_def, condition)
    measure_def.filter = condition
    return measure_def
end

-- =============================================================================
-- Cardinality Constants
-- =============================================================================

ONE_TO_ONE = "one_to_one"
ONE_TO_MANY = "one_to_many"
MANY_TO_ONE = "many_to_one"
MANY_TO_MANY = "many_to_many"

-- Short aliases
O2O = "one_to_one"
O2M = "one_to_many"
M2O = "many_to_one"
M2M = "many_to_many"

-- =============================================================================
-- Materialization Constants
-- =============================================================================

VIEW = "view"
TABLE = "table"
INCREMENTAL = "incremental"
SNAPSHOT = "snapshot"

-- =============================================================================
-- Include Selection Constants
-- =============================================================================

ALL = "*"  -- Include all columns from an entity

-- =============================================================================
-- SCD Type Constants
-- =============================================================================

SCD_TYPE_0 = 0  -- Retain original (no changes)
SCD_TYPE_1 = 1  -- Overwrite (no history)
SCD_TYPE_2 = 2  -- Add new row (full history)

SCD0 = 0
SCD1 = 1
SCD2 = 2

-- =============================================================================
-- Change Tracking Constants
-- =============================================================================

APPEND_ONLY = "append_only"
CDC = "cdc"
FULL_SNAPSHOT = "full_snapshot"

-- =============================================================================
-- Entity Reference System
-- =============================================================================

-- Metatable for entity references that captures entity.column syntax
local ref_metatable = {
    __index = function(self, column)
        return self._entity .. "." .. column
    end,
    __tostring = function(self)
        return self._entity
    end,
    __concat = function(a, b)
        return tostring(a) .. tostring(b)
    end
}

--- Create an entity reference for use in relationships and grain
-- @param entity_name Name of the source entity
-- @return Reference proxy object
function ref(entity_name)
    return setmetatable({ _entity = entity_name }, ref_metatable)
end

-- Registry of defined sources for auto-ref creation
local _defined_sources = {}

--- Check if a source has been defined
-- @param name Source name
-- @return boolean
function source_exists(name)
    return _defined_sources[name] == true
end

--- Get all defined source names
-- @return Array of source names
function get_sources()
    local names = {}
    for name, _ in pairs(_defined_sources) do
        lua_table.insert(names, name)
    end
    return names
end

-- =============================================================================
-- Enhanced Source Definition
-- =============================================================================

-- The Rust source() function now returns a builder for chained syntax:
--   source("orders"):from("raw.orders"):columns({...})
-- The :from() method registers the source and creates the global ref.
-- We just need to track defined sources for introspection.

local _original_source = source
source = function(name)
    -- Track that this source is being defined
    _defined_sources[name] = true
    -- Return the builder from Rust (supports :from(), :columns(), :metadata(), etc.)
    return _original_source(name)
end

-- =============================================================================
-- Intermediate Definition (DEPRECATED - use table() with table_type = "Staging")
-- =============================================================================
-- intermediate() has been removed. Use table() instead:
--   table("stg_orders", { from = "orders", table_type = "Staging" })
--       :columns({...})
--       :filter("status = 'completed'")

-- For backwards compatibility, we provide a wrapper that converts to table()
local _defined_tables_as_intermediates = {}

--- DEPRECATED: Use table() with table_type = "Staging" instead
-- @param name Table name
-- @return Table builder
function intermediate(name)
    -- Track for introspection
    _defined_tables_as_intermediates[name] = true
    -- Return a table builder with Staging type
    return table(name, { table_type = "Staging" })
end

--- Get all defined tables that were created via intermediate()
function get_intermediates()
    return _defined_tables_as_intermediates
end

-- =============================================================================
-- Dimension Definition
-- =============================================================================
-- The Rust dimension() function now returns a builder for chained syntax:
--   dimension("dim_customers"):target("table"):from("customers"):columns({...})
-- The builder methods register the dimension and create the global ref.

local _defined_dimensions = {}
local _original_dimension = dimension

--- Define a dimension using chained syntax
-- @param name Dimension name
-- @return Builder table with :target(), :from(), :columns(), :primary_key(), :scd(), :materialized()
dimension = function(name)
    -- Track that this dimension is being defined
    _defined_dimensions[name] = true
    -- Return the builder from Rust
    return _original_dimension(name)
end

--- Get all defined dimensions
function get_dimensions()
    return _defined_dimensions
end

-- =============================================================================
-- Fact Definition
-- =============================================================================
-- The Rust fact() function now returns a builder for chained syntax:
--   fact("fact_orders"):target("table"):grain({...}):measure("name", sum("col"))
-- The builder methods register the fact and create the global ref.

local _defined_facts = {}
local _original_fact = fact

--- Define a fact using chained syntax
-- @param name Fact name
-- @return Builder table with :target(), :grain(), :include(), :measure(), :materialized(), :incremental()
fact = function(name)
    -- Track that this fact is being defined
    _defined_facts[name] = true
    -- Return the builder from Rust
    return _original_fact(name)
end

--- Get all defined facts
function get_facts()
    return _defined_facts
end

-- =============================================================================
-- Relationship Helpers
-- =============================================================================

--- Shorthand for defining a relationship
-- @param from_ref From reference (entity.column)
-- @param to_ref To reference (entity.column)
-- @param card Cardinality (optional, defaults to MANY_TO_ONE)
--
-- Basic usage:
--   link(orders.customer_id, customers.customer_id)
function link(from_ref, to_ref, card)
    relationship {
        from = from_ref,
        to = to_ref,
        cardinality = card or MANY_TO_ONE,
    }
end

--- Define a relationship with a role name (for role-playing dimensions)
-- Use this when a fact has multiple FKs to the same dimension.
-- @param from_ref From reference (entity.column)
-- @param to_ref To reference (entity.column)
-- @param role_name Role name (e.g., "order_date", "ship_date")
-- @param card Cardinality (optional, defaults to MANY_TO_ONE)
--
-- Example:
--   link_as(orders.order_date_id, date.date_id, "order_date")
--   link_as(orders.ship_date_id, date.date_id, "ship_date")
--
-- In queries, use the role name to disambiguate:
--   query { order_date.month, ship_date.month, revenue }
function link_as(from_ref, to_ref, role_name, card)
    relationship {
        from = from_ref,
        to = to_ref,
        cardinality = card or MANY_TO_ONE,
        role = role_name,
    }
end

--- Define a many-to-one relationship
function many_to_one(from_ref, to_ref)
    link(from_ref, to_ref, MANY_TO_ONE)
end

--- Define a one-to-many relationship
function one_to_many(from_ref, to_ref)
    link(from_ref, to_ref, ONE_TO_MANY)
end

--- Define a one-to-one relationship
function one_to_one(from_ref, to_ref)
    link(from_ref, to_ref, ONE_TO_ONE)
end

--- Define a many-to-many relationship
function many_to_many(from_ref, to_ref)
    link(from_ref, to_ref, MANY_TO_MANY)
end

-- =============================================================================
-- Include Helpers
-- =============================================================================

--- Include all columns from an entity (placeholder, resolved at compile time)
-- @return Special marker for "all columns"
function all()
    return "*"
end

--- Exclude specific columns (include all except these)
-- @param columns Array of column names to exclude
-- @return Exclusion marker
function except(columns)
    return { _except = columns }
end

-- =============================================================================
-- Grain Helpers
-- =============================================================================

--- Create a grain definition from multiple entity.column refs
-- @param ... Entity.column references
-- @return Array suitable for grain field
function grain(...)
    local result = {}
    for _, ref in ipairs({...}) do
        lua_table.insert(result, ref)
    end
    return result
end

-- =============================================================================
-- Utility Functions
-- =============================================================================

--- Merge two tables (shallow)
-- @param t1 First table
-- @param t2 Second table (values override t1)
-- @return Merged table
function merge(t1, t2)
    local result = {}
    for k, v in pairs(t1 or {}) do
        result[k] = v
    end
    for k, v in pairs(t2 or {}) do
        result[k] = v
    end
    return result
end

--- Create array from arguments
-- @param ... Values
-- @return Array table
function array(...)
    return {...}
end

--- Check if value is in array
-- @param val Value to find
-- @param arr Array to search
-- @return boolean
function contains(arr, val)
    for _, v in ipairs(arr) do
        if v == val then return true end
    end
    return false
end

-- =============================================================================
-- Environment / Config Helpers
-- =============================================================================

--- Get environment variable with optional default
-- @param name Environment variable name
-- @param default Default value if not set
-- @return Value or default
function env(name, default)
    return os.getenv(name) or default
end

--- Conditional value based on environment
-- @param env_name Environment variable to check
-- @param if_true Value if env var is set and truthy
-- @param if_false Value if env var is not set or falsy
-- @return Selected value
function env_switch(env_name, if_true, if_false)
    local val = os.getenv(env_name)
    if val and val ~= "" and val ~= "0" and val:lower() ~= "false" then
        return if_true
    end
    return if_false
end

-- =============================================================================
-- Debug Helpers (stripped in production?)
-- =============================================================================

--- Pretty print a table (for debugging)
-- @param tbl Table to print
-- @param indent Current indentation level
function dump(tbl, indent)
    indent = indent or 0
    local prefix = string.rep("  ", indent)
    if type(tbl) ~= "table" then
        print(prefix .. tostring(tbl))
        return
    end
    for k, v in pairs(tbl) do
        if type(v) == "table" then
            print(prefix .. tostring(k) .. " = {")
            dump(v, indent + 1)
            print(prefix .. "}")
        else
            print(prefix .. tostring(k) .. " = " .. tostring(v))
        end
    end
end

-- =============================================================================
-- Expression Builders
-- =============================================================================
-- With the new SQL string support, expressions can be written as plain SQL:
--   filter = "status != 'cancelled' AND total > 0"
--   computed = "quantity * unit_price * (1 - discount / 100)"
--
-- The Rust loader will parse SQL strings automatically using sqlparser.
-- The builder functions below are still available for programmatic expression
-- construction, but most use cases can now use simpler SQL strings.

-- Metatable for expression objects (enables operator overloading)
local expr_mt = {
    -- Arithmetic operators
    __add = function(a, b)
        return { _expr = "binary", op = "+", left = a, right = b }
    end,
    __sub = function(a, b)
        return { _expr = "binary", op = "-", left = a, right = b }
    end,
    __mul = function(a, b)
        return { _expr = "binary", op = "*", left = a, right = b }
    end,
    __div = function(a, b)
        return { _expr = "binary", op = "/", left = a, right = b }
    end,
    __mod = function(a, b)
        return { _expr = "binary", op = "%", left = a, right = b }
    end,
    __unm = function(a)
        return { _expr = "unary", op = "-", expr = a }
    end,
    __concat = function(a, b)
        return { _expr = "binary", op = "||", left = a, right = b }
    end,
    -- Comparison operators (Lua 5.3+)
    __eq = function(a, b)
        return setmetatable({ _expr = "binary", op = "=", left = a, right = b }, expr_mt)
    end,
    __lt = function(a, b)
        return setmetatable({ _expr = "binary", op = "<", left = a, right = b }, expr_mt)
    end,
    __le = function(a, b)
        return setmetatable({ _expr = "binary", op = "<=", left = a, right = b }, expr_mt)
    end,
}

-- Helper to wrap a value as an expression if needed
local function as_expr(val)
    if type(val) == "table" and val._expr then
        return val  -- Already an expression
    elseif type(val) == "string" then
        -- Pass strings through - Rust will determine if it's SQL or column ref
        return val
    elseif type(val) == "number" then
        if math.floor(val) == val then
            return setmetatable({ _expr = "literal", value = val, type = "int" }, expr_mt)
        else
            return setmetatable({ _expr = "literal", value = val, type = "float" }, expr_mt)
        end
    elseif type(val) == "boolean" then
        return setmetatable({ _expr = "literal", value = val, type = "bool" }, expr_mt)
    elseif val == nil then
        return setmetatable({ _expr = "literal", value = nil, type = "null" }, expr_mt)
    end
    return val
end

--- Create a column reference expression
-- @param name Column name, or "entity.column" for qualified reference
-- @return Expression node
function col(name)
    if type(name) == "string" and name:match("^[%w_]+%.[%w_]+$") then
        local entity, column = name:match("^([%w_]+)%.([%w_]+)$")
        return setmetatable({ _expr = "column", entity = entity, column = column }, expr_mt)
    end
    return setmetatable({ _expr = "column", column = name }, expr_mt)
end

--- Create a literal value expression
-- @param value The literal value
-- @return Expression node
function lit(value)
    return as_expr(value)
end

--- Create a SQL expression (explicit)
-- Use this to force a string to be parsed as SQL even if it doesn't
-- contain obvious SQL operators.
-- @param sql SQL expression string
-- @return Expression node that Rust will parse with sqlparser
function sql(sql_str)
    return { _expr = "sql", sql = sql_str }
end

-- =============================================================================
-- Comparison Functions
-- =============================================================================
-- Since Lua's == and ~= can't be overloaded to return expressions,
-- we provide these helper functions for building filter expressions.

--- Equal comparison
function eq(a, b)
    return setmetatable({ _expr = "binary", op = "=", left = as_expr(a), right = as_expr(b) }, expr_mt)
end

--- Not equal comparison
function ne(a, b)
    return setmetatable({ _expr = "binary", op = "!=", left = as_expr(a), right = as_expr(b) }, expr_mt)
end

--- Greater than comparison
function gt(a, b)
    return setmetatable({ _expr = "binary", op = ">", left = as_expr(a), right = as_expr(b) }, expr_mt)
end

--- Greater than or equal comparison
function gte(a, b)
    return setmetatable({ _expr = "binary", op = ">=", left = as_expr(a), right = as_expr(b) }, expr_mt)
end

--- Less than comparison
function lt(a, b)
    return setmetatable({ _expr = "binary", op = "<", left = as_expr(a), right = as_expr(b) }, expr_mt)
end

--- Less than or equal comparison
function lte(a, b)
    return setmetatable({ _expr = "binary", op = "<=", left = as_expr(a), right = as_expr(b) }, expr_mt)
end

--- LIKE pattern matching
function like(a, pattern)
    return setmetatable({ _expr = "binary", op = "LIKE", left = as_expr(a), right = as_expr(pattern) }, expr_mt)
end

--- NOT LIKE pattern matching
function not_like(a, pattern)
    return setmetatable({ _expr = "binary", op = "NOT LIKE", left = as_expr(a), right = as_expr(pattern) }, expr_mt)
end

--- IS NULL check
function is_null(a)
    return setmetatable({ _expr = "unary", op = "IS NULL", expr = as_expr(a) }, expr_mt)
end

--- IS NOT NULL check
function is_not_null(a)
    return setmetatable({ _expr = "unary", op = "IS NOT NULL", expr = as_expr(a) }, expr_mt)
end

--- IN list check
function is_in(a, values)
    local val_exprs = {}
    for _, v in ipairs(values) do
        lua_table.insert(val_exprs, as_expr(v))
    end
    return setmetatable({ _expr = "in", expr = as_expr(a), values = val_exprs }, expr_mt)
end

--- AND combinator for expressions
function AND(...)
    local exprs = {...}
    if #exprs == 0 then return nil end
    local result = as_expr(exprs[1])
    for i = 2, #exprs do
        result = setmetatable({ _expr = "binary", op = "AND", left = result, right = as_expr(exprs[i]) }, expr_mt)
    end
    return result
end

--- OR combinator for expressions
function OR(...)
    local exprs = {...}
    if #exprs == 0 then return nil end
    local result = as_expr(exprs[1])
    for i = 2, #exprs do
        result = setmetatable({ _expr = "binary", op = "OR", left = result, right = as_expr(exprs[i]) }, expr_mt)
    end
    return result
end

--- NOT expression
function NOT(a)
    return setmetatable({ _expr = "unary", op = "NOT", expr = as_expr(a) }, expr_mt)
end

--- Cast an expression to a type
-- @param expr Expression to cast
-- @param target_type Target data type
-- @return Expression node
function cast(expr, target_type)
    return setmetatable({
        _expr = "cast",
        expr = as_expr(expr),
        target_type = target_type
    }, expr_mt)
end

--- Return first non-null value
-- @param ... Expressions to coalesce
-- @return Expression node
function coalesce(...)
    local args = {}
    for _, v in ipairs({...}) do
        lua_table.insert(args, as_expr(v))
    end
    return setmetatable({ _expr = "function", func = "coalesce", args = args }, expr_mt)
end

--- Return null if two values are equal
-- @param expr1 First expression
-- @param expr2 Second expression
-- @return Expression node
function nullif(expr1, expr2)
    return setmetatable({
        _expr = "function",
        func = "nullif",
        args = { as_expr(expr1), as_expr(expr2) }
    }, expr_mt)
end

--- Create a CASE WHEN expression
-- @param whens Array of {when = condition, then_ = result} tables
-- @param else_val Optional else value
-- @return Expression node
function case_when(whens, else_val)
    local when_clauses = {}
    for _, w in ipairs(whens) do
        lua_table.insert(when_clauses, {
            when = as_expr(w.when),
            then_ = as_expr(w.then_)
        })
    end
    return setmetatable({
        _expr = "case",
        when_clauses = when_clauses,
        else_clause = else_val and as_expr(else_val) or nil
    }, expr_mt)
end

-- Case builder metatable for method chaining
local case_builder_mt = {}
case_builder_mt.__index = case_builder_mt

--- Add a when clause to the case expression
-- @param condition SQL condition string or expression
-- @param result SQL result string or expression
-- @return self for chaining
function case_builder_mt:when(condition, result)
    -- Wrap string conditions as SQL expressions
    local cond_expr = type(condition) == "string" and sql(condition) or as_expr(condition)
    local result_expr = type(result) == "string" and sql(result) or as_expr(result)

    lua_table.insert(self._when_clauses, {
        when = cond_expr,
        then_ = result_expr
    })
    return self
end

--- Set the else clause and finalize the case expression
-- @param value SQL string or expression for the else branch
-- @return Expression node
function case_builder_mt:else_(value)
    local else_expr = type(value) == "string" and sql(value) or as_expr(value)
    return setmetatable({
        _expr = "case",
        when_clauses = self._when_clauses,
        else_clause = else_expr
    }, expr_mt)
end

--- Finalize case without else (NULL for non-matching)
-- @return Expression node
function case_builder_mt:done()
    return setmetatable({
        _expr = "case",
        when_clauses = self._when_clauses,
        else_clause = nil
    }, expr_mt)
end

--- Create a case expression builder with fluent syntax
-- Usage: case():when("amount < 100", "'small'"):when("amount < 1000", "'medium'"):else_("'large'")
-- @return Case builder object
function case()
    return setmetatable({
        _when_clauses = {}
    }, case_builder_mt)
end

-- =============================================================================
-- SQL Functions as Expressions
-- =============================================================================

--- Generic SQL function call
-- @param name Function name
-- @param ... Arguments
-- @return Expression node
local function sql_func(name, ...)
    local args = {}
    for _, v in ipairs({...}) do
        lua_table.insert(args, as_expr(v))
    end
    return setmetatable({ _expr = "function", func = name, args = args }, expr_mt)
end

-- String functions
function concat(...) return sql_func("concat", ...) end
function substring(str, start, len) return sql_func("substring", str, start, len) end
function trim(str) return sql_func("trim", str) end
function ltrim(str) return sql_func("ltrim", str) end
function rtrim(str) return sql_func("rtrim", str) end
function upper(str) return sql_func("upper", str) end
function lower(str) return sql_func("lower", str) end
function initcap(str) return sql_func("initcap", str) end
function length(str) return sql_func("length", str) end
function replace(str, from, to) return sql_func("replace", str, from, to) end
function regexp_replace(str, pattern, replacement) return sql_func("regexp_replace", str, pattern, replacement) end
function position(substr, str) return sql_func("position", substr, str) end

-- Date functions
function date_trunc(part, expr) return sql_func("date_trunc", part, expr) end
function datediff(part, start_date, end_date) return sql_func("datediff", part, start_date, end_date) end
function dateadd(part, amount, expr) return sql_func("dateadd", part, amount, expr) end
function current_date() return sql_func("current_date") end
function current_timestamp() return sql_func("current_timestamp") end

-- Math functions
function abs(expr) return sql_func("abs", expr) end
function round(expr, decimals) return sql_func("round", expr, decimals or 0) end
function floor(expr) return sql_func("floor", expr) end
function ceil(expr) return sql_func("ceil", expr) end

-- Try cast (returns null on failure)
function try_cast(expr, target_type)
    return setmetatable({
        _expr = "function",
        func = "try_cast",
        args = { as_expr(expr) },
        target_type = target_type
    }, expr_mt)
end

-- =============================================================================
-- Window Functions
-- =============================================================================
-- Window functions support method chaining for defining OVER clauses:
--   order_rank = row_number():partition_by("customer_id"):order_by("order_date")
--   running_total = sum("amount"):over():partition_by("customer_id"):order_by("date")
--
-- The Rust loader expects tables with these fields:
--   func: window function name (required)
--   args: array of expressions (optional)
--   partition_by: array of partition expressions (optional)
--   order_by: array of { col, dir } tables (optional)
--   frame: { type, start, end_ } table (optional)

-- Metatable for window function definitions with method chaining
local window_mt = {
    __index = {
        --- Add PARTITION BY columns
        -- @param ... Column names or expressions to partition by
        -- @return The modified window function (for chaining)
        partition_by = function(self, ...)
            -- Use rawget/rawset to avoid metatable lookup conflicts
            if not rawget(self, "partition_by") then
                rawset(self, "partition_by", {})
            end
            for _, col in ipairs({...}) do
                lua_table.insert(rawget(self, "partition_by"), col)
            end
            return self
        end,

        --- Add ORDER BY clause
        -- @param col Column name or expression
        -- @param dir Sort direction (ASC or DESC, default ASC)
        -- @return The modified window function (for chaining)
        order_by = function(self, col, dir)
            -- Use rawget/rawset to avoid metatable lookup conflicts
            if not rawget(self, "order_by") then
                rawset(self, "order_by", {})
            end
            lua_table.insert(rawget(self, "order_by"), { expr = col, dir = dir or ASC })
            return self
        end,

        --- Set window frame using ROWS
        -- @param start_bound Start bound (e.g., "unbounded preceding", "current row", "3 preceding")
        -- @param end_bound End bound (optional, defaults to "current row")
        -- @return The modified window function (for chaining)
        rows = function(self, start_bound, end_bound)
            self.frame = {
                kind = "rows",
                start = start_bound,
                ["end"] = end_bound or "current row"
            }
            return self
        end,

        --- Set window frame using RANGE
        -- @param start_bound Start bound
        -- @param end_bound End bound (optional)
        -- @return The modified window function (for chaining)
        range = function(self, start_bound, end_bound)
            self.frame = {
                kind = "range",
                start = start_bound,
                ["end"] = end_bound or "current row"
            }
            return self
        end,
    }
}

--- Create a window function (used in window_columns)
-- @param func_name Window function name
-- @param args Optional arguments
-- @return Window function definition with chaining methods
local function window_func(func_name, ...)
    local args = {}
    for _, v in ipairs({...}) do
        lua_table.insert(args, as_expr(v))
    end
    return setmetatable({ _window = true, func = func_name, args = args }, window_mt)
end

-- Ranking window functions
function row_number() return window_func("row_number") end
function rank() return window_func("rank") end
function dense_rank() return window_func("dense_rank") end
function ntile(n) return window_func("ntile", n) end
function percent_rank() return window_func("percent_rank") end
function cume_dist() return window_func("cume_dist") end

-- Value window functions
function lag(expr, offset, default) return window_func("lag", expr, offset or 1, default) end
function lead(expr, offset, default) return window_func("lead", expr, offset or 1, default) end
function first_value(expr) return window_func("first_value", expr) end
function last_value(expr) return window_func("last_value", expr) end
function nth_value(expr, n) return window_func("nth_value", expr, n) end

-- =============================================================================
-- Sort Direction Constants
-- =============================================================================

ASC = "asc"
DESC = "desc"

-- =============================================================================
-- Dedup Keep Constants
-- =============================================================================

FIRST = "first"
LAST = "last"

-- =============================================================================
-- Column Definition Helpers
-- =============================================================================

--- Rename a column (for intermediates/facts)
-- @param source_col Source column name
-- @param target_col Target column name
-- @return Column definition
function rename(source_col, target_col)
    return { _coldef = "renamed", source = source_col, target = target_col }
end

--- Create a computed column (for intermediates/facts)
-- @param name Column name
-- @param expr Expression
-- @param data_type Optional data type
-- @return Column definition
function compute(name, expr, data_type)
    return { _coldef = "computed", name = name, expr = as_expr(expr), data_type = data_type }
end

-- =============================================================================
-- Query DSL Helpers
-- =============================================================================
-- These helpers enable the query {} syntax for defining semantic queries.
--
-- Example:
--   query "sales_by_region" {
--       select = {
--           customers.region,
--           customers.segment,
--           "revenue",
--           "order_count",
--       },
--       where = {
--           gte(date.year, 2024),
--           eq(customers.segment, "Enterprise"),
--       },
--       order_by = { desc("revenue"), asc(customers.region) },
--       limit = 100,
--   }

--- Create a descending order specification
-- @param field Field name or entity.column reference
-- @return Order specification table
function desc(field)
    local field_str = type(field) == "string" and field or tostring(field)
    return { _order = true, field = field_str, dir = "desc" }
end

--- Create an ascending order specification
-- @param field Field name or entity.column reference
-- @return Order specification table
function asc(field)
    local field_str = type(field) == "string" and field or tostring(field)
    return { _order = true, field = field_str, dir = "asc" }
end

-- Metatable for filtered measures (returned by :where())
-- Supports :as() for setting alias after filtering
local filtered_measure_mt = {
    __index = {
        --- Set an alias for the filtered measure
        -- @param alias_name Output alias
        -- @return Modified filtered measure reference (for chaining)
        as = function(self, alias_name)
            self.alias = alias_name
            return self
        end
    }
}

-- Metatable for measure references with :where() support for filtered measures
local measure_ref_mt = {
    __index = {
        --- Add a filter condition to a measure reference
        -- Creates a filtered measure that generates CASE WHEN expressions
        -- @param ... Filter conditions (from filter() helper)
        -- @return Filtered measure reference table with :as() method
        where = function(self, ...)
            local filters = {...}
            return setmetatable({
                _filtered_measure = true,
                name = self.name,
                alias = self.alias,
                filters = filters
            }, filtered_measure_mt)
        end,
        --- Set an alias for the measure
        -- @param alias_name Output alias
        -- @return Modified measure reference (for chaining)
        as = function(self, alias_name)
            self.alias = alias_name
            return self
        end
    }
}

--- Reference a measure by name
-- @param name Measure name (must be defined in a fact)
-- @return Measure reference table with :where() and :as() methods
function measure(name)
    return setmetatable({ _measure_ref = true, name = name }, measure_ref_mt)
end

--- Create a derived measure (inline calculation from other measures)
-- Derived measures are computed after aggregation.
-- @param alias Output alias for the derived measure
-- @param expression Expression using measure references and operators
-- @return Derived measure definition table
--
-- Example:
--   derived("aov", m.revenue / m.order_count)
--   derived("margin_pct", (m.revenue - m.cost) / m.revenue * 100)
function derived(alias, expression)
    return {
        _derived_measure = true,
        alias = alias,
        expression = expression
    }
end

-- Metatable for measure reference objects in derived expressions
-- Declared first so it can reference itself for chaining
local m_ref_mt
m_ref_mt = {
    __add = function(a, b)
        return setmetatable({ _derived_op = "+", left = a, right = b }, m_ref_mt)
    end,
    __sub = function(a, b)
        return setmetatable({ _derived_op = "-", left = a, right = b }, m_ref_mt)
    end,
    __mul = function(a, b)
        return setmetatable({ _derived_op = "*", left = a, right = b }, m_ref_mt)
    end,
    __div = function(a, b)
        return setmetatable({ _derived_op = "/", left = a, right = b }, m_ref_mt)
    end,
    __unm = function(a)
        return setmetatable({ _derived_op = "negate", expr = a }, m_ref_mt)
    end
}

--- Create a measure reference for use in derived expressions
-- @param name Measure name
-- @return Measure reference with arithmetic operators
function m(name)
    return setmetatable({ _measure_ref_expr = true, name = name }, m_ref_mt)
end

--- Reference a dimension column
-- @param entity Entity name
-- @param column Column name
-- @return Dimension reference string
function dim(entity, column)
    return entity .. "." .. column
end

--- Create a filter condition (alternative to eq/gte/etc.)
-- @param field Field reference (entity.column string)
-- @param op Comparison operator
-- @param value Value to compare against
-- @return Filter specification table
function filter(field, op, value)
    local field_str = type(field) == "string" and field or tostring(field)
    return { _filter = true, field = field_str, op = op, value = value }
end

--- IN list filter
-- @param field Field reference
-- @param ... Values for the IN list
-- @return Filter specification table
function is_in(field, ...)
    local field_str = type(field) == "string" and field or tostring(field)
    return { _filter = true, field = field_str, op = "in", value = {...} }
end

--- NOT IN list filter
-- @param field Field reference
-- @param ... Values for the NOT IN list
-- @return Filter specification table
function is_not_in(field, ...)
    local field_str = type(field) == "string" and field or tostring(field)
    return { _filter = true, field = field_str, op = "not_in", value = {...} }
end

--- BETWEEN filter
-- @param field Field reference
-- @param low Lower bound
-- @param high Upper bound
-- @return Filter specification table
function between(field, low, high)
    local field_str = type(field) == "string" and field or tostring(field)
    return { _filter = true, field = field_str, op = "between", low = low, high = high }
end

--- IS NULL filter
-- @param field Field reference
-- @return Filter specification table
function is_null(field)
    local field_str = type(field) == "string" and field or tostring(field)
    return { _filter = true, field = field_str, op = "is_null" }
end

--- IS NOT NULL filter
-- @param field Field reference
-- @return Filter specification table
function is_not_null(field)
    local field_str = type(field) == "string" and field or tostring(field)
    return { _filter = true, field = field_str, op = "is_not_null" }
end

-- =============================================================================
-- Time Intelligence Functions
-- =============================================================================

--- Year-to-date: cumulative sum from start of year
-- @param measure Measure name or reference
-- @param opts Optional table with { year = "col", period = "col", via = "role" }
-- @return Time function specification
--
-- Example:
--   ytd(revenue)                                     -- Uses date config defaults
--   ytd(revenue, { via = "order_date" })            -- Specify date role
--   ytd(revenue, { year = "fiscal_year", period = "fiscal_month" })
function ytd(measure, opts)
    opts = opts or {}
    local measure_name = type(measure) == "table" and measure.name or measure
    return setmetatable({
        _time_fn = "ytd",
        measure = measure_name,
        year_column = opts.year,
        period_column = opts.period,
        via = opts.via,
    }, m_ref_mt)
end

--- Quarter-to-date: cumulative sum from start of quarter
-- @param measure Measure name or reference
-- @param opts Optional table with { year = "col", quarter = "col", period = "col", via = "role" }
-- @return Time function specification
function qtd(measure, opts)
    opts = opts or {}
    local measure_name = type(measure) == "table" and measure.name or measure
    return setmetatable({
        _time_fn = "qtd",
        measure = measure_name,
        year_column = opts.year,
        quarter_column = opts.quarter,
        period_column = opts.period,
        via = opts.via,
    }, m_ref_mt)
end

--- Month-to-date: cumulative sum from start of month
-- @param measure Measure name or reference
-- @param opts Optional table with { year = "col", month = "col", day = "col", via = "role" }
-- @return Time function specification
function mtd(measure, opts)
    opts = opts or {}
    local measure_name = type(measure) == "table" and measure.name or measure
    return setmetatable({
        _time_fn = "mtd",
        measure = measure_name,
        year_column = opts.year,
        month_column = opts.month,
        day_column = opts.day,
        via = opts.via,
    }, m_ref_mt)
end

--- Prior period: value from N periods ago
-- @param measure Measure name or reference
-- @param periods Number of periods back (default: 1)
-- @param opts Optional table with { via = "role" }
-- @return Time function specification
--
-- Example:
--   prior_period(revenue)       -- Previous period
--   prior_period(revenue, 3)    -- 3 periods ago
function prior_period(measure, periods, opts)
    opts = opts or {}
    local measure_name = type(measure) == "table" and measure.name or measure
    return setmetatable({
        _time_fn = "prior_period",
        measure = measure_name,
        periods_back = periods or 1,
        via = opts.via,
    }, m_ref_mt)
end

--- Prior year: same period in the prior year
-- @param measure Measure name or reference
-- @param opts Optional table with { via = "role" }
-- @return Time function specification
--
-- Example:
--   prior_year(revenue)                     -- Same period last year
--   prior_year(revenue, { via = "ship_date" })  -- Via ship date
function prior_year(measure, opts)
    opts = opts or {}
    local measure_name = type(measure) == "table" and measure.name or measure
    return setmetatable({
        _time_fn = "prior_year",
        measure = measure_name,
        via = opts.via,
    }, m_ref_mt)
end

--- Prior quarter: same period in the prior quarter
-- @param measure Measure name or reference
-- @param opts Optional table with { via = "role" }
-- @return Time function specification
function prior_quarter(measure, opts)
    opts = opts or {}
    local measure_name = type(measure) == "table" and measure.name or measure
    return setmetatable({
        _time_fn = "prior_quarter",
        measure = measure_name,
        via = opts.via,
    }, m_ref_mt)
end

--- Rolling sum: sum over the last N periods
-- @param measure Measure name or reference
-- @param periods Number of periods to include (including current)
-- @param opts Optional table with { via = "role" }
-- @return Time function specification
--
-- Example:
--   rolling_sum(revenue, 3)    -- Sum of current + previous 2 periods
--   rolling_sum(revenue, 12)   -- Rolling 12-month sum
function rolling_sum(measure, periods, opts)
    opts = opts or {}
    local measure_name = type(measure) == "table" and measure.name or measure
    return setmetatable({
        _time_fn = "rolling_sum",
        measure = measure_name,
        periods = periods,
        via = opts.via,
    }, m_ref_mt)
end

--- Rolling average: average over the last N periods
-- @param measure Measure name or reference
-- @param periods Number of periods to include (including current)
-- @param opts Optional table with { via = "role" }
-- @return Time function specification
--
-- Example:
--   rolling_avg(revenue, 3)    -- 3-period moving average
--   rolling_avg(revenue, 12)   -- 12-month moving average
function rolling_avg(measure, periods, opts)
    opts = opts or {}
    local measure_name = type(measure) == "table" and measure.name or measure
    return setmetatable({
        _time_fn = "rolling_avg",
        measure = measure_name,
        periods = periods,
        via = opts.via,
    }, m_ref_mt)
end

--- Delta: difference between current and previous value
-- @param current Current value (measure or time function)
-- @param previous Previous value (usually a time function like prior_year)
-- @return Delta expression
--
-- Example:
--   delta(revenue, prior_year(revenue))  -- revenue - prior_year_revenue
function delta(current, previous)
    return setmetatable({
        _time_fn = "delta",
        current = current,
        previous = previous,
    }, m_ref_mt)
end

--- Growth: percentage change from previous to current
-- Calculates (current - previous) / previous * 100
-- Returns NULL if previous is 0 (avoids division by zero)
-- @param current Current value (measure or time function)
-- @param previous Previous value (usually a time function like prior_year)
-- @return Growth expression
--
-- Example:
--   growth(revenue, prior_year(revenue))  -- YoY growth percentage
function growth(current, previous)
    return setmetatable({
        _time_fn = "growth",
        current = current,
        previous = previous,
    }, m_ref_mt)
end
