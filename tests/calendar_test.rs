// tests/model/calendar_test.rs
#[cfg(test)]
mod tests {
    use mantis_core::model::{Calendar, CalendarBody, DrillPath, GrainLevel, PhysicalCalendar};
    use std::collections::HashMap;

    #[test]
    fn test_physical_calendar() {
        let mut grain_mappings = HashMap::new();
        grain_mappings.insert(GrainLevel::Day, "date_key".to_string());
        grain_mappings.insert(GrainLevel::Month, "month_start_date".to_string());
        grain_mappings.insert(GrainLevel::Year, "year_start_date".to_string());

        let mut drill_paths = HashMap::new();
        drill_paths.insert(
            "standard".to_string(),
            DrillPath {
                name: "standard".to_string(),
                levels: vec![GrainLevel::Day, GrainLevel::Month, GrainLevel::Year],
            },
        );

        let calendar = Calendar {
            name: "dates".to_string(),
            body: CalendarBody::Physical(PhysicalCalendar {
                source: "dbo.dim_date".to_string(),
                grain_mappings,
                drill_paths,
                fiscal_year_start: None,
                week_start: None,
            }),
        };

        assert_eq!(calendar.name, "dates");

        // Test supports_grain
        assert!(calendar.supports_grain(GrainLevel::Day));
        assert!(calendar.supports_grain(GrainLevel::Month));
        assert!(!calendar.supports_grain(GrainLevel::Week)); // Not in mappings
    }

    #[test]
    fn test_grain_column_lookup() {
        let mut grain_mappings = HashMap::new();
        grain_mappings.insert(GrainLevel::Day, "date_key".to_string());
        grain_mappings.insert(GrainLevel::Month, "month_start_date".to_string());

        let calendar = Calendar {
            name: "dates".to_string(),
            body: CalendarBody::Physical(PhysicalCalendar {
                source: "dbo.dim_date".to_string(),
                grain_mappings,
                drill_paths: HashMap::new(),
                fiscal_year_start: None,
                week_start: None,
            }),
        };

        assert_eq!(calendar.grain_column(GrainLevel::Day).unwrap(), "date_key");
        assert_eq!(
            calendar.grain_column(GrainLevel::Month).unwrap(),
            "month_start_date"
        );
        assert!(calendar.grain_column(GrainLevel::Week).is_err());
    }
}
