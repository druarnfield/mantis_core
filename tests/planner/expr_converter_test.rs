use mantis::planner::expr_converter::QueryContext;

#[test]
fn test_query_context_table_aliases() {
    let mut ctx = QueryContext::new();
    ctx.add_table("users".to_string(), "u".to_string());
    ctx.add_table("orders".to_string(), "o".to_string());

    assert_eq!(ctx.get_table_alias("users").unwrap(), "u");
    assert_eq!(ctx.get_table_alias("orders").unwrap(), "o");
    assert!(ctx.get_table_alias("unknown").is_err());
}
