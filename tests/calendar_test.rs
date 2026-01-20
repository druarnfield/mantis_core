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

    #[test]
    fn test_generated_calendar() {
        let calendar = Calendar {
            name: "auto_dates".to_string(),
            body: CalendarBody::Generated {
                grain: GrainLevel::Day,
                from: "2020-01-01".to_string(),
                to: "2025-12-31".to_string(),
            },
        };

        assert_eq!(calendar.name, "auto_dates");
        assert!(calendar.supports_grain(GrainLevel::Day));
        assert!(!calendar.supports_grain(GrainLevel::Month));
        assert_eq!(
            calendar.grain_column(GrainLevel::Day).unwrap(),
            "generated_date"
        );
        assert!(calendar.grain_column(GrainLevel::Month).is_err());
    }

    #[test]
    fn test_grain_column_error_messages() {
        let calendar = Calendar {
            name: "dates".to_string(),
            body: CalendarBody::Physical(PhysicalCalendar {
                source: "dbo.dim_date".to_string(),
                grain_mappings: {
                    let mut map = HashMap::new();
                    map.insert(GrainLevel::Day, "date_key".to_string());
                    map
                },
                drill_paths: HashMap::new(),
                fiscal_year_start: None,
                week_start: None,
            }),
        };

        let err = calendar.grain_column(GrainLevel::Week).unwrap_err();
        assert!(err.contains("dates"));
        assert!(err.contains("week") || err.contains("Week"));
    }

    #[test]
    fn test_drill_paths() {
        let fiscal_path = DrillPath {
            name: "fiscal".to_string(),
            levels: vec![
                GrainLevel::FiscalYear,
                GrainLevel::FiscalQuarter,
                GrainLevel::FiscalMonth,
            ],
        };

        assert_eq!(fiscal_path.name, "fiscal");
        assert_eq!(fiscal_path.levels.len(), 3);
        assert_eq!(fiscal_path.levels[0], GrainLevel::FiscalYear);
        assert_eq!(fiscal_path.levels[2], GrainLevel::FiscalMonth);
    }

    #[test]
    fn test_fiscal_year_and_week_start() {
        use mantis_core::model::{Month, Weekday};

        let calendar = Calendar {
            name: "fiscal_dates".to_string(),
            body: CalendarBody::Physical(PhysicalCalendar {
                source: "dbo.dim_date".to_string(),
                grain_mappings: HashMap::new(),
                drill_paths: HashMap::new(),
                fiscal_year_start: Some(Month::July),
                week_start: Some(Weekday::Monday),
            }),
        };

        if let CalendarBody::Physical(phys) = &calendar.body {
            assert_eq!(phys.fiscal_year_start, Some(Month::July));
            assert_eq!(phys.week_start, Some(Weekday::Monday));
        } else {
            panic!("Expected Physical calendar");
        }
    }
}
