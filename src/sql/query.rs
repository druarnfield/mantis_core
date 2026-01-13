//! Query builder - construct SQL queries with a fluent API.

use super::dialect::{Dialect, SqlDialect};
use super::expr::{Expr, ExprExt};
use super::token::{Token, TokenStream};

// =============================================================================
// Select Expression (column with optional alias)
// =============================================================================

/// A SELECT list item: expression with optional alias.
#[derive(Debug, Clone, PartialEq)]
#[must_use = "builders have no effect until used"]
pub struct SelectExpr {
    pub expr: Expr,
    pub alias: Option<String>,
}

impl SelectExpr {
    pub fn new(expr: Expr) -> Self {
        Self { expr, alias: None }
    }

    pub fn with_alias(mut self, alias: &str) -> Self {
        self.alias = Some(alias.into());
        self
    }

    pub fn to_tokens(&self) -> TokenStream {
        self.to_tokens_for_dialect(Dialect::default())
    }

    pub fn to_tokens_for_dialect(&self, dialect: Dialect) -> TokenStream {
        let mut ts = self.expr.to_tokens_for_dialect(dialect);
        if let Some(alias) = &self.alias {
            ts.space()
                .push(Token::As)
                .space()
                .push(Token::Ident(alias.clone()));
        }
        ts
    }
}

impl From<Expr> for SelectExpr {
    fn from(expr: Expr) -> Self {
        SelectExpr::new(expr)
    }
}

// =============================================================================
// Table Reference
// =============================================================================

/// A table reference with optional schema and alias.
#[derive(Debug, Clone, PartialEq)]
#[must_use = "builders have no effect until used"]
pub struct TableRef {
    pub schema: Option<String>,
    pub table: String,
    pub alias: Option<String>,
}

impl TableRef {
    pub fn new(table: &str) -> Self {
        Self {
            schema: None,
            table: table.into(),
            alias: None,
        }
    }

    pub fn with_schema(mut self, schema: &str) -> Self {
        self.schema = Some(schema.into());
        self
    }

    pub fn with_alias(mut self, alias: &str) -> Self {
        self.alias = Some(alias.into());
        self
    }

    pub fn to_tokens(&self) -> TokenStream {
        let mut ts = TokenStream::new();
        ts.push(Token::QualifiedIdent {
            schema: self.schema.clone(),
            name: self.table.clone(),
        });
        if let Some(alias) = &self.alias {
            ts.space()
                .push(Token::As)
                .space()
                .push(Token::Ident(alias.clone()));
        }
        ts
    }
}

// =============================================================================
// Joins
// =============================================================================

/// Type of join.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinType {
    Inner,
    Left,
    Right,
    Full,
    Cross,
}

/// A JOIN clause.
#[derive(Debug, Clone, PartialEq)]
pub struct Join {
    pub join_type: JoinType,
    pub table: TableRef,
    pub on: Option<Expr>,
}

impl Join {
    pub fn to_tokens(&self) -> TokenStream {
        self.to_tokens_for_dialect(Dialect::default())
    }

    pub fn to_tokens_for_dialect(&self, dialect: Dialect) -> TokenStream {
        let mut ts = TokenStream::new();

        match self.join_type {
            JoinType::Inner => ts.push(Token::Inner),
            JoinType::Left => ts.push(Token::Left),
            JoinType::Right => ts.push(Token::Right),
            JoinType::Full => ts.push(Token::Full).space().push(Token::Outer),
            JoinType::Cross => ts.push(Token::Cross),
        };

        ts.space().push(Token::Join).space();
        ts.append(&self.table.to_tokens());

        if let Some(on) = &self.on {
            ts.space().push(Token::On).space();
            ts.append(&on.to_tokens_for_dialect(dialect));
        }

        ts
    }
}

// =============================================================================
// ORDER BY
// =============================================================================

/// Sort direction.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SortDir {
    #[default]
    Asc,
    Desc,
}

/// NULLS ordering.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NullsOrder {
    First,
    Last,
}

/// An ORDER BY expression.
#[derive(Debug, Clone, PartialEq)]
#[must_use = "builders have no effect until used"]
pub struct OrderByExpr {
    pub expr: Expr,
    pub dir: Option<SortDir>,
    pub nulls: Option<NullsOrder>,
}

impl OrderByExpr {
    pub fn new(expr: Expr) -> Self {
        Self {
            expr,
            dir: None,
            nulls: None,
        }
    }

    pub fn asc(expr: Expr) -> Self {
        Self {
            expr,
            dir: Some(SortDir::Asc),
            nulls: None,
        }
    }

    pub fn desc(expr: Expr) -> Self {
        Self {
            expr,
            dir: Some(SortDir::Desc),
            nulls: None,
        }
    }

    pub fn nulls_first(mut self) -> Self {
        self.nulls = Some(NullsOrder::First);
        self
    }

    pub fn nulls_last(mut self) -> Self {
        self.nulls = Some(NullsOrder::Last);
        self
    }

    /// Convert to tokens (dialect-agnostic, may emit invalid SQL for some dialects).
    pub fn to_tokens(&self) -> TokenStream {
        self.to_tokens_for_dialect(Dialect::default())
    }

    /// Convert to tokens for a specific dialect.
    ///
    /// Skips NULLS FIRST/LAST for dialects that don't support it.
    pub fn to_tokens_for_dialect(&self, dialect: Dialect) -> TokenStream {
        let mut ts = self.expr.to_tokens();

        if let Some(dir) = &self.dir {
            ts.space().push(match dir {
                SortDir::Asc => Token::Asc,
                SortDir::Desc => Token::Desc,
            });
        }

        // Only emit NULLS FIRST/LAST if dialect supports it
        if let Some(nulls) = &self.nulls {
            if dialect.supports_nulls_ordering() {
                ts.space().push(match nulls {
                    NullsOrder::First => Token::NullsFirst,
                    NullsOrder::Last => Token::NullsLast,
                });
            }
            // For dialects without NULLS ordering support, we silently skip it.
            // TODO: Consider emulating with CASE expressions for MySQL/older T-SQL
        }

        ts
    }
}

// =============================================================================
// LIMIT / OFFSET
// =============================================================================

/// LIMIT and OFFSET clause.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct LimitOffset {
    pub limit: Option<u64>,
    pub offset: Option<u64>,
}

impl LimitOffset {
    /// Convert to token stream using dialect-specific pagination.
    ///
    /// Delegates to `SqlDialect::emit_limit_offset()` for the actual formatting.
    pub fn to_tokens(&self, dialect: Dialect) -> TokenStream {
        dialect.emit_limit_offset(self.limit, self.offset)
    }
}

// =============================================================================
// Set Operations (UNION, INTERSECT, EXCEPT)
// =============================================================================

/// Type of set operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SetOpType {
    Union,
    Intersect,
    Except,
}

/// A set operation combining two queries.
#[derive(Debug, Clone, PartialEq)]
#[must_use = "SetOperation has no effect until converted to SQL with to_sql()"]
pub struct SetOperation {
    pub left: Box<Query>,
    pub op: SetOpType,
    pub all: bool,
    pub right: Box<Query>,
}

impl SetOperation {
    /// Create a UNION operation.
    pub fn union(left: Query, right: Query) -> Self {
        Self {
            left: Box::new(left),
            op: SetOpType::Union,
            all: false,
            right: Box::new(right),
        }
    }

    /// Create a UNION ALL operation.
    pub fn union_all(left: Query, right: Query) -> Self {
        Self {
            left: Box::new(left),
            op: SetOpType::Union,
            all: true,
            right: Box::new(right),
        }
    }

    /// Create an INTERSECT operation.
    pub fn intersect(left: Query, right: Query) -> Self {
        Self {
            left: Box::new(left),
            op: SetOpType::Intersect,
            all: false,
            right: Box::new(right),
        }
    }

    /// Create an INTERSECT ALL operation.
    pub fn intersect_all(left: Query, right: Query) -> Self {
        Self {
            left: Box::new(left),
            op: SetOpType::Intersect,
            all: true,
            right: Box::new(right),
        }
    }

    /// Create an EXCEPT operation.
    pub fn except(left: Query, right: Query) -> Self {
        Self {
            left: Box::new(left),
            op: SetOpType::Except,
            all: false,
            right: Box::new(right),
        }
    }

    /// Create an EXCEPT ALL operation.
    pub fn except_all(left: Query, right: Query) -> Self {
        Self {
            left: Box::new(left),
            op: SetOpType::Except,
            all: true,
            right: Box::new(right),
        }
    }

    /// Chain another set operation (returns a new SetOperation with this as left).
    pub fn chain(self, op: SetOpType, all: bool, right: Query) -> Self {
        // Wrap the current operation as the "left" query
        let left_query = Query {
            set_op: Some(Box::new(self)),
            ..Default::default()
        };
        Self {
            left: Box::new(left_query),
            op,
            all,
            right: Box::new(right),
        }
    }

    /// Convert to tokens for a specific dialect.
    pub fn to_tokens_for_dialect(&self, dialect: Dialect) -> TokenStream {
        let mut ts = TokenStream::new();

        // Left query (wrapped in parens for clarity)
        if self.left.set_op.is_some() {
            // Nested set operation - emit directly
            ts.append(&self.left.to_tokens_for_dialect(dialect));
        } else {
            ts.lparen();
            ts.append(&self.left.to_tokens_for_dialect(dialect));
            ts.rparen();
        }

        // Set operation keyword
        ts.newline();
        ts.push(match self.op {
            SetOpType::Union => Token::Union,
            SetOpType::Intersect => Token::Intersect,
            SetOpType::Except => Token::Except,
        });
        if self.all {
            ts.space().push(Token::All);
        }
        ts.newline();

        // Right query
        ts.lparen();
        ts.append(&self.right.to_tokens_for_dialect(dialect));
        ts.rparen();

        ts
    }

    /// Generate SQL string for a specific dialect.
    pub fn to_sql(&self, dialect: Dialect) -> String {
        self.to_tokens_for_dialect(dialect).serialize(dialect)
    }
}

impl std::fmt::Display for SetOperation {
    /// Formats the set operation using the default dialect (DuckDB).
    ///
    /// For dialect-specific SQL, use [`SetOperation::to_sql`] instead.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.to_sql(Dialect::default()))
    }
}

// =============================================================================
// CTE (Common Table Expression)
// =============================================================================

/// A Common Table Expression (WITH clause).
#[derive(Debug, Clone, PartialEq)]
#[must_use = "builders have no effect until used"]
pub struct Cte {
    pub name: String,
    pub columns: Option<Vec<String>>,
    pub query: Box<Query>,
    /// Whether this is a recursive CTE.
    pub recursive: bool,
}

impl Cte {
    pub fn new(name: &str, query: Query) -> Self {
        Self {
            name: name.into(),
            columns: None,
            query: Box::new(query),
            recursive: false,
        }
    }

    /// Create a recursive CTE.
    pub fn recursive(name: &str, query: Query) -> Self {
        Self {
            name: name.into(),
            columns: None,
            query: Box::new(query),
            recursive: true,
        }
    }

    pub fn with_columns(mut self, columns: Vec<&str>) -> Self {
        self.columns = Some(columns.into_iter().map(String::from).collect());
        self
    }

    pub fn to_tokens(&self) -> TokenStream {
        self.to_tokens_for_dialect(Dialect::default())
    }

    pub fn to_tokens_for_dialect(&self, dialect: Dialect) -> TokenStream {
        let mut ts = TokenStream::new();
        ts.push(Token::Ident(self.name.clone()));

        if let Some(cols) = &self.columns {
            ts.space().lparen();
            for (i, col) in cols.iter().enumerate() {
                if i > 0 {
                    ts.comma().space();
                }
                ts.push(Token::Ident(col.clone()));
            }
            ts.rparen();
        }

        ts.space()
            .push(Token::As)
            .space()
            .lparen()
            .newline()
            .append(&self.query.to_tokens_for_dialect(dialect))
            .newline()
            .rparen();

        ts
    }
}

// =============================================================================
// Query Builder
// =============================================================================

/// A SELECT query.
#[derive(Debug, Clone, Default, PartialEq)]
#[must_use = "Query has no effect until converted to SQL with to_sql() or to_tokens()"]
pub struct Query {
    pub with: Vec<Cte>,
    pub select: Vec<SelectExpr>,
    pub distinct: bool,
    pub from: Option<TableRef>,
    pub joins: Vec<Join>,
    pub where_clause: Option<Expr>,
    pub group_by: Vec<Expr>,
    pub having: Option<Expr>,
    pub order_by: Vec<OrderByExpr>,
    pub limit_offset: Option<LimitOffset>,
    /// Set operation (UNION, INTERSECT, EXCEPT) with another query.
    pub set_op: Option<Box<SetOperation>>,
}

impl Query {
    /// Create a new empty query.
    pub fn new() -> Self {
        Self::default()
    }

    /// Add a CTE (WITH clause).
    pub fn with_cte(mut self, cte: Cte) -> Self {
        self.with.push(cte);
        self
    }

    /// Set the SELECT list.
    pub fn select(mut self, exprs: Vec<impl Into<SelectExpr>>) -> Self {
        self.select = exprs.into_iter().map(|e| e.into()).collect();
        self
    }

    /// SELECT *
    pub fn select_star(mut self) -> Self {
        self.select = vec![SelectExpr::new(crate::expr::star())];
        self
    }

    /// Add DISTINCT.
    pub fn distinct(mut self) -> Self {
        self.distinct = true;
        self
    }

    /// Set the FROM table.
    pub fn from(mut self, table: TableRef) -> Self {
        self.from = Some(table);
        self
    }

    /// Add a JOIN.
    pub fn join(mut self, join_type: JoinType, table: TableRef, on: Expr) -> Self {
        self.joins.push(Join {
            join_type,
            table,
            on: Some(on),
        });
        self
    }

    /// Add an INNER JOIN.
    pub fn inner_join(self, table: TableRef, on: Expr) -> Self {
        self.join(JoinType::Inner, table, on)
    }

    /// Add a LEFT JOIN.
    pub fn left_join(self, table: TableRef, on: Expr) -> Self {
        self.join(JoinType::Left, table, on)
    }

    /// Add a RIGHT JOIN.
    pub fn right_join(self, table: TableRef, on: Expr) -> Self {
        self.join(JoinType::Right, table, on)
    }

    /// Add a FULL OUTER JOIN.
    pub fn full_join(self, table: TableRef, on: Expr) -> Self {
        self.join(JoinType::Full, table, on)
    }

    /// Add a CROSS JOIN.
    pub fn cross_join(mut self, table: TableRef) -> Self {
        self.joins.push(Join {
            join_type: JoinType::Cross,
            table,
            on: None,
        });
        self
    }

    /// Add a WHERE condition (ANDed with existing conditions).
    pub fn filter(mut self, condition: Expr) -> Self {
        self.where_clause = Some(match self.where_clause {
            Some(existing) => existing.and(condition),
            None => condition,
        });
        self
    }

    /// Set the GROUP BY clause.
    pub fn group_by(mut self, exprs: Vec<Expr>) -> Self {
        self.group_by = exprs;
        self
    }

    /// Set the HAVING clause.
    pub fn having(mut self, condition: Expr) -> Self {
        self.having = Some(condition);
        self
    }

    /// Set the ORDER BY clause.
    pub fn order_by(mut self, exprs: Vec<OrderByExpr>) -> Self {
        self.order_by = exprs;
        self
    }

    /// Set LIMIT.
    pub fn limit(mut self, limit: u64) -> Self {
        match &mut self.limit_offset {
            Some(lo) => lo.limit = Some(limit),
            None => {
                self.limit_offset = Some(LimitOffset {
                    limit: Some(limit),
                    offset: None,
                })
            }
        }
        self
    }

    /// Set OFFSET.
    pub fn offset(mut self, offset: u64) -> Self {
        match &mut self.limit_offset {
            Some(lo) => lo.offset = Some(offset),
            None => {
                self.limit_offset = Some(LimitOffset {
                    limit: None,
                    offset: Some(offset),
                })
            }
        }
        self
    }

    // =========================================================================
    // Set Operations
    // =========================================================================

    /// Combine with another query using UNION.
    pub fn union(self, other: Query) -> SetOperation {
        SetOperation::union(self, other)
    }

    /// Combine with another query using UNION ALL.
    pub fn union_all(self, other: Query) -> SetOperation {
        SetOperation::union_all(self, other)
    }

    /// Combine with another query using INTERSECT.
    pub fn intersect(self, other: Query) -> SetOperation {
        SetOperation::intersect(self, other)
    }

    /// Combine with another query using INTERSECT ALL.
    pub fn intersect_all(self, other: Query) -> SetOperation {
        SetOperation::intersect_all(self, other)
    }

    /// Combine with another query using EXCEPT.
    pub fn except(self, other: Query) -> SetOperation {
        SetOperation::except(self, other)
    }

    /// Combine with another query using EXCEPT ALL.
    pub fn except_all(self, other: Query) -> SetOperation {
        SetOperation::except_all(self, other)
    }

    /// Convert to token stream (dialect-agnostic, uses DuckDB for LIMIT/OFFSET).
    pub fn to_tokens(&self) -> TokenStream {
        self.to_tokens_for_dialect(Dialect::DuckDb)
    }

    /// Convert to token stream for a specific dialect.
    pub fn to_tokens_for_dialect(&self, dialect: Dialect) -> TokenStream {
        // If this query is a container for a set operation, emit that instead
        if let Some(ref set_op) = self.set_op {
            return set_op.to_tokens_for_dialect(dialect);
        }

        let mut ts = TokenStream::new();

        // WITH clause
        if !self.with.is_empty() {
            ts.push(Token::With);

            // Emit RECURSIVE keyword if any CTE is recursive AND dialect supports it
            let has_recursive = self.with.iter().any(|cte| cte.recursive);
            if has_recursive && dialect.emit_recursive_keyword() {
                ts.space().push(Token::Recursive);
            }

            ts.space();
            for (i, cte) in self.with.iter().enumerate() {
                if i > 0 {
                    ts.comma().newline();
                }
                ts.append(&cte.to_tokens_for_dialect(dialect));
            }
            ts.newline();
        }

        // SELECT
        ts.push(Token::Select);
        if self.distinct {
            ts.space().push(Token::Distinct);
        }

        // Columns
        for (i, select_expr) in self.select.iter().enumerate() {
            if i == 0 {
                ts.newline().indent(1);
            } else {
                ts.comma().newline().indent(1);
            }
            ts.append(&select_expr.to_tokens_for_dialect(dialect));
        }

        // FROM
        if let Some(from) = &self.from {
            ts.newline().push(Token::From).space();
            ts.append(&from.to_tokens());
        }

        // JOINs
        for join in &self.joins {
            ts.newline();
            ts.append(&join.to_tokens_for_dialect(dialect));
        }

        // WHERE
        if let Some(where_clause) = &self.where_clause {
            ts.newline().push(Token::Where).space();
            ts.append(&where_clause.to_tokens_for_dialect(dialect));
        }

        // GROUP BY
        if !self.group_by.is_empty() {
            ts.newline().push(Token::GroupBy).space();
            for (i, expr) in self.group_by.iter().enumerate() {
                if i > 0 {
                    ts.comma().space();
                }
                ts.append(&expr.to_tokens_for_dialect(dialect));
            }
        }

        // HAVING
        if let Some(having) = &self.having {
            ts.newline().push(Token::Having).space();
            ts.append(&having.to_tokens_for_dialect(dialect));
        }

        // ORDER BY
        // Note: T-SQL requires ORDER BY for OFFSET FETCH syntax.
        // If ORDER BY is missing but we have LIMIT/OFFSET, emit ORDER BY (SELECT NULL).
        let needs_order_by_placeholder = dialect.requires_order_by_for_offset()
            && self.order_by.is_empty()
            && self.limit_offset.is_some();

        if !self.order_by.is_empty() {
            ts.newline().push(Token::OrderBy).space();
            for (i, order_expr) in self.order_by.iter().enumerate() {
                if i > 0 {
                    ts.comma().space();
                }
                ts.append(&order_expr.to_tokens_for_dialect(dialect));
            }
        } else if needs_order_by_placeholder {
            // T-SQL requires ORDER BY for OFFSET FETCH syntax. When no ORDER BY is
            // specified but LIMIT/OFFSET is used, we emit `ORDER BY (SELECT NULL)`
            // as a syntactically valid placeholder.
            //
            // WARNING: This makes row ordering non-deterministic. The database may
            // return rows in any order, which can cause inconsistent pagination.
            // For predictable results, always specify an explicit ORDER BY clause
            // when using LIMIT/OFFSET.
            ts.newline()
                .push(Token::OrderBy)
                .space()
                .lparen()
                .push(Token::Select)
                .space()
                .push(Token::Null)
                .rparen();
        }

        // LIMIT / OFFSET
        if let Some(lo) = &self.limit_offset {
            ts.newline();
            ts.append(&lo.to_tokens(dialect));
        }

        ts
    }

    /// Generate SQL string for a specific dialect.
    pub fn to_sql(&self, dialect: Dialect) -> String {
        self.to_tokens_for_dialect(dialect).serialize(dialect)
    }
}

impl std::fmt::Display for Query {
    /// Formats the query using the default dialect (DuckDB).
    ///
    /// For dialect-specific SQL, use [`Query::to_sql`] instead.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.to_sql(Dialect::default()))
    }
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::expr::{col, count_star, lit_int, sum, table_col};

    #[test]
    fn test_simple_select() {
        let query = Query::new()
            .select(vec![col("id"), col("name")])
            .from(TableRef::new("users").with_schema("dbo"));

        let sql = query.to_sql(Dialect::TSql);
        assert!(sql.contains("[dbo].[users]"));
        assert!(sql.contains("[id]"));
        assert!(sql.contains("[name]"));
    }

    #[test]
    fn test_select_star() {
        let query = Query::new().select_star().from(TableRef::new("users"));

        let sql = query.to_sql(Dialect::Postgres);
        assert!(sql.contains("*"));
    }

    #[test]
    fn test_filter() {
        let query = Query::new()
            .select(vec![col("name")])
            .from(TableRef::new("users"))
            .filter(col("active").eq(true))
            .filter(col("age").gte(lit_int(18)));

        let sql = query.to_sql(Dialect::DuckDb);
        assert!(sql.contains("WHERE"));
        assert!(sql.contains("AND"));
        assert!(sql.contains("true"));
        assert!(sql.contains("18"));
    }

    #[test]
    fn test_join() {
        let query = Query::new()
            .select(vec![table_col("u", "name"), table_col("o", "total")])
            .from(TableRef::new("users").with_alias("u"))
            .inner_join(
                TableRef::new("orders").with_alias("o"),
                table_col("u", "id").eq(table_col("o", "user_id")),
            );

        let sql = query.to_sql(Dialect::MySql);
        assert!(sql.contains("INNER JOIN"));
        assert!(sql.contains("ON"));
    }

    #[test]
    fn test_aggregation() {
        let query = Query::new()
            .select(vec![
                col("region").into(),
                sum(col("amount")).alias("total"),
                count_star().alias("cnt"),
            ])
            .from(TableRef::new("orders"))
            .group_by(vec![col("region")])
            .having(sum(col("amount")).gt(lit_int(1000)));

        let sql = query.to_sql(Dialect::Postgres);
        assert!(sql.contains("GROUP BY"));
        assert!(sql.contains("HAVING"));
        assert!(sql.contains("SUM"));
    }

    #[test]
    fn test_order_by() {
        let query = Query::new()
            .select(vec![col("name"), col("age")])
            .from(TableRef::new("users"))
            .order_by(vec![
                OrderByExpr::desc(col("age")),
                OrderByExpr::asc(col("name")),
            ]);

        let sql = query.to_sql(Dialect::Postgres);
        assert!(sql.contains("ORDER BY"));
        assert!(sql.contains("DESC"));
        assert!(sql.contains("ASC"));
    }

    #[test]
    fn test_limit_duckdb() {
        let query = Query::new()
            .select_star()
            .from(TableRef::new("users"))
            .order_by(vec![OrderByExpr::asc(col("id"))])
            .limit(10)
            .offset(20);

        let sql = query.to_sql(Dialect::DuckDb);
        assert!(sql.contains("LIMIT 10"));
        assert!(sql.contains("OFFSET 20"));
    }

    #[test]
    fn test_limit_tsql() {
        let query = Query::new()
            .select_star()
            .from(TableRef::new("users"))
            .order_by(vec![OrderByExpr::asc(col("id"))])
            .limit(10)
            .offset(20);

        let sql = query.to_sql(Dialect::TSql);
        assert!(sql.contains("OFFSET 20 ROWS"));
        assert!(sql.contains("FETCH NEXT 10 ROWS ONLY"));
    }

    #[test]
    fn test_limit_tsql_without_order_by() {
        // T-SQL requires ORDER BY for OFFSET/FETCH, so we use ORDER BY (SELECT NULL)
        let query = Query::new()
            .select_star()
            .from(TableRef::new("users"))
            .limit(10);

        let sql = query.to_sql(Dialect::TSql);
        assert!(
            sql.contains("ORDER BY (SELECT NULL)"),
            "Expected ORDER BY (SELECT NULL) placeholder, got: {}",
            sql
        );
        assert!(sql.contains("OFFSET 0 ROWS"));
        assert!(sql.contains("FETCH NEXT 10 ROWS ONLY"));
    }

    #[test]
    fn test_distinct() {
        let query = Query::new()
            .select(vec![col("category")])
            .distinct()
            .from(TableRef::new("products"));

        let sql = query.to_sql(Dialect::Postgres);
        assert!(sql.contains("SELECT DISTINCT"));
    }

    #[test]
    fn test_cte() {
        let inner = Query::new()
            .select(vec![
                col("region").into(),
                sum(col("amount")).alias("total"),
            ])
            .from(TableRef::new("orders"))
            .group_by(vec![col("region")]);

        let query = Query::new()
            .with_cte(Cte::new("regional_totals", inner))
            .select_star()
            .from(TableRef::new("regional_totals"))
            .filter(col("total").gt(lit_int(10000)));

        let sql = query.to_sql(Dialect::Postgres);
        assert!(sql.contains("WITH"));
        assert!(sql.contains("regional_totals"));
        assert!(sql.contains("AS"));
    }

    #[test]
    fn test_aliased_columns() {
        let query = Query::new()
            .select(vec![
                col("first_name").alias("fname"),
                col("last_name").alias("lname"),
            ])
            .from(TableRef::new("users"));

        let sql = query.to_sql(Dialect::Postgres);
        assert!(sql.contains("AS \"fname\""));
        assert!(sql.contains("AS \"lname\""));
    }

    #[test]
    fn test_subquery_in_filter() {
        let subquery = Query::new()
            .select(vec![col("user_id")])
            .from(TableRef::new("orders"))
            .filter(col("total").gt(lit_int(1000)));

        let query = Query::new()
            .select_star()
            .from(TableRef::new("users"))
            .filter(Expr::InSubquery {
                expr: Box::new(col("id")),
                subquery: Box::new(subquery),
                negated: false,
            });

        let sql = query.to_sql(Dialect::Postgres);
        assert!(sql.contains("IN"));
        assert!(sql.contains("SELECT"));
    }

    // Set operation tests
    #[test]
    fn test_union() {
        let q1 = Query::new()
            .select(vec![col("name"), col("email")])
            .from(TableRef::new("customers"));

        let q2 = Query::new()
            .select(vec![col("name"), col("email")])
            .from(TableRef::new("suppliers"));

        let combined = q1.union(q2);
        let sql = combined.to_sql(Dialect::Postgres);

        assert!(sql.contains("UNION"));
        assert!(sql.contains("customers"));
        assert!(sql.contains("suppliers"));
        assert!(!sql.contains("ALL"));
    }

    #[test]
    fn test_union_all() {
        let q1 = Query::new()
            .select(vec![col("id")])
            .from(TableRef::new("table1"));

        let q2 = Query::new()
            .select(vec![col("id")])
            .from(TableRef::new("table2"));

        let combined = q1.union_all(q2);
        let sql = combined.to_sql(Dialect::DuckDb);

        assert!(sql.contains("UNION ALL"));
    }

    #[test]
    fn test_intersect() {
        let q1 = Query::new()
            .select(vec![col("product_id")])
            .from(TableRef::new("orders"));

        let q2 = Query::new()
            .select(vec![col("product_id")])
            .from(TableRef::new("returns"));

        let combined = q1.intersect(q2);
        let sql = combined.to_sql(Dialect::Postgres);

        assert!(sql.contains("INTERSECT"));
        assert!(!sql.contains("ALL"));
    }

    #[test]
    fn test_except() {
        let q1 = Query::new()
            .select(vec![col("user_id")])
            .from(TableRef::new("all_users"));

        let q2 = Query::new()
            .select(vec![col("user_id")])
            .from(TableRef::new("banned_users"));

        let combined = q1.except(q2);
        let sql = combined.to_sql(Dialect::Postgres);

        assert!(sql.contains("EXCEPT"));
    }

    #[test]
    fn test_chained_set_operations() {
        let q1 = Query::new()
            .select(vec![col("id")])
            .from(TableRef::new("t1"));

        let q2 = Query::new()
            .select(vec![col("id")])
            .from(TableRef::new("t2"));

        let q3 = Query::new()
            .select(vec![col("id")])
            .from(TableRef::new("t3"));

        // q1 UNION q2 UNION ALL q3
        let combined = q1.union(q2).chain(SetOpType::Union, true, q3);
        let sql = combined.to_sql(Dialect::DuckDb);

        assert!(sql.contains("t1"));
        assert!(sql.contains("t2"));
        assert!(sql.contains("t3"));
        assert!(sql.contains("UNION ALL"));
    }

    #[test]
    fn test_set_operation_tsql() {
        let q1 = Query::new()
            .select(vec![col("name")])
            .from(TableRef::new("employees"));

        let q2 = Query::new()
            .select(vec![col("name")])
            .from(TableRef::new("contractors"));

        let combined = q1.union_all(q2);
        let sql = combined.to_sql(Dialect::TSql);

        // Verify T-SQL quoting is used
        assert!(sql.contains("[name]"));
        assert!(sql.contains("[employees]"));
        assert!(sql.contains("UNION ALL"));
    }

    #[test]
    fn test_query_display() {
        let query = Query::new()
            .select(vec![col("id"), col("name")])
            .from(TableRef::new("users"));

        // Display uses default dialect (DuckDB)
        let display_sql = format!("{}", query);
        assert!(display_sql.contains("SELECT"));
        assert!(display_sql.contains("\"id\""));
        assert!(display_sql.contains("\"users\""));
    }

    #[test]
    fn test_set_operation_display() {
        let q1 = Query::new()
            .select(vec![col("id")])
            .from(TableRef::new("t1"));

        let q2 = Query::new()
            .select(vec![col("id")])
            .from(TableRef::new("t2"));

        let combined = q1.union(q2);

        // Display uses default dialect (DuckDB)
        let display_sql = format!("{}", combined);
        assert!(display_sql.contains("UNION"));
        assert!(display_sql.contains("\"t1\""));
        assert!(display_sql.contains("\"t2\""));
    }
}
