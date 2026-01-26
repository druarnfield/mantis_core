use super::{PlanError, PlanResult};
use std::collections::HashMap;

pub struct QueryContext {
    table_aliases: HashMap<String, String>,
}

impl QueryContext {
    pub fn new() -> Self {
        Self {
            table_aliases: HashMap::new(),
        }
    }

    pub fn add_table(&mut self, entity: String, alias: String) {
        self.table_aliases.insert(entity, alias);
    }

    pub fn get_table_alias(&self, entity: &str) -> PlanResult<&str> {
        self.table_aliases
            .get(entity)
            .map(|s| s.as_str())
            .ok_or_else(|| PlanError::LogicalPlanError(format!("Unknown entity: {}", entity)))
    }
}
