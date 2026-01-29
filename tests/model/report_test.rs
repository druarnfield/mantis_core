#[cfg(test)]
mod tests {
    use mantis::model::{GroupItem, Report, ShowItem, TimeSuffix};

    #[test]
    fn test_report_with_drill_path_group() {
        let group = vec![GroupItem::DrillPathRef {
            source: "dates".to_string(),
            path: "standard".to_string(),
            level: "month".to_string(),
            label: Some("Month".to_string()),
        }];

        let report = Report {
            name: "test".to_string(),
            from: vec!["fact_sales".to_string()],
            use_date: vec!["order_date_id".to_string()],
            period: None,
            group,
            show: vec![],
            filters: vec![],
            sort: vec![],
            limit: None,
        };

        assert_eq!(report.name, "test");
        assert_eq!(report.group.len(), 1);
    }

    #[test]
    fn test_report_with_time_suffix() {
        let show = vec![ShowItem::MeasureWithSuffix {
            name: "revenue".to_string(),
            suffix: TimeSuffix::Ytd,
            label: Some("YTD Revenue".to_string()),
        }];

        let report = Report {
            name: "test".to_string(),
            from: vec!["fact_sales".to_string()],
            use_date: vec!["order_date_id".to_string()],
            period: None,
            group: vec![],
            show,
            filters: vec![],
            sort: vec![],
            limit: None,
        };

        assert_eq!(report.show.len(), 1);
        assert!(matches!(report.show[0], ShowItem::MeasureWithSuffix { .. }));
    }
}
