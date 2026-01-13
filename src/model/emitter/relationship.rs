//! Relationship → Lua emission.

use super::format::IndentWriter;
use super::EmitConfig;
use crate::model::{Cardinality, Relationship, RelationshipSource};

/// Convert cardinality to Lua string representation.
fn cardinality_to_lua(card: Cardinality) -> &'static str {
    match card {
        Cardinality::OneToOne => "one_to_one",
        Cardinality::OneToMany => "one_to_many",
        Cardinality::ManyToOne => "many_to_one",
        Cardinality::ManyToMany => "many_to_many",
        Cardinality::Unknown => "many_to_one", // Default fallback
    }
}

/// Emit a relationship to Lua.
///
/// Uses short form (link) for simple many-to-one relationships without comments,
/// and long form (relationship block) for others.
pub fn emit_relationship(w: &mut IndentWriter, rel: &Relationship, config: &EmitConfig) {
    // Comment for relationship source
    if config.include_comments {
        match &rel.source {
            RelationshipSource::ForeignKey => {
                w.write_comment("Foreign key constraint");
            }
            RelationshipSource::Inferred { rule, confidence } => {
                w.write_comment(&format!(
                    "Inferred (confidence: {:.2}, rule: {})",
                    confidence, rule
                ));
            }
            RelationshipSource::Explicit => {
                // User-defined, no comment needed
            }
        }
    }

    // Use short form for simple many-to-one relationships without comments
    let use_short_form = rel.cardinality == Cardinality::ManyToOne && !config.include_comments;

    if use_short_form {
        w.write_line(&format!(
            "link({}.{}, {}.{})",
            rel.from_entity, rel.from_column, rel.to_entity, rel.to_column
        ));
        return;
    }

    // Long form: relationship block
    w.write_line("relationship {");
    w.indent();
    w.write_line(&format!(
        "from = \"{}.{}\",",
        rel.from_entity, rel.from_column
    ));
    w.write_line(&format!("to = \"{}.{}\",", rel.to_entity, rel.to_column));
    w.write_line(&format!(
        "cardinality = \"{}\",",
        cardinality_to_lua(rel.cardinality)
    ));
    w.dedent();
    w.write_line("}");
}

/// Emit multiple relationships, optionally grouping by source type.
#[allow(dead_code)]
pub fn emit_relationships(
    w: &mut IndentWriter,
    relationships: &[&Relationship],
    config: &EmitConfig,
) {
    if !config.include_comments {
        // Simple case: emit all relationships without grouping
        for rel in relationships {
            emit_relationship(w, rel, config);
        }
        return;
    }

    // Group by source type when comments are enabled
    let (fk_rels, inferred_rels, explicit_rels): (Vec<_>, Vec<_>, Vec<_>) = relationships.iter().fold(
        (vec![], vec![], vec![]),
        |(mut fk, mut inferred, mut explicit), rel| {
            match &rel.source {
                RelationshipSource::ForeignKey => fk.push(*rel),
                RelationshipSource::Inferred { .. } => inferred.push(*rel),
                RelationshipSource::Explicit => explicit.push(*rel),
            }
            (fk, inferred, explicit)
        },
    );

    // Foreign key relationships first
    if !fk_rels.is_empty() {
        w.write_comment("Foreign key constraints");
        w.blank_line();
        for rel in fk_rels {
            emit_relationship(w, rel, &EmitConfig::minimal());
        }
        w.blank_line();
    }

    // Explicit relationships
    if !explicit_rels.is_empty() {
        w.write_comment("Explicitly defined relationships");
        w.blank_line();
        for rel in explicit_rels {
            emit_relationship(w, rel, &EmitConfig::minimal());
        }
        w.blank_line();
    }

    // Inferred relationships
    if !inferred_rels.is_empty() {
        w.write_comment("Inferred relationships");
        w.blank_line();
        for rel in inferred_rels {
            emit_relationship(w, rel, config);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::emitter::format::Indent;

    #[test]
    fn test_cardinality_to_lua() {
        assert_eq!(cardinality_to_lua(Cardinality::OneToOne), "one_to_one");
        assert_eq!(cardinality_to_lua(Cardinality::OneToMany), "one_to_many");
        assert_eq!(cardinality_to_lua(Cardinality::ManyToOne), "many_to_one");
        assert_eq!(cardinality_to_lua(Cardinality::ManyToMany), "many_to_many");
        assert_eq!(cardinality_to_lua(Cardinality::Unknown), "many_to_one");
    }

    #[test]
    fn test_emit_relationship_short_form() {
        let rel = Relationship::new(
            "orders",
            "customers",
            "customer_id",
            "customer_id",
            Cardinality::ManyToOne,
        );

        let config = EmitConfig::minimal();
        let mut w = IndentWriter::new(Indent::Tabs);

        emit_relationship(&mut w, &rel, &config);
        let output = w.into_string();

        assert_eq!(
            output.trim(),
            "link(orders.customer_id, customers.customer_id)"
        );
    }

    #[test]
    fn test_emit_relationship_long_form_with_comments() {
        let rel = Relationship::new(
            "orders",
            "customers",
            "customer_id",
            "customer_id",
            Cardinality::ManyToOne,
        );

        let config = EmitConfig::default();
        let mut w = IndentWriter::new(Indent::Tabs);

        emit_relationship(&mut w, &rel, &config);
        let output = w.into_string();

        assert!(output.contains("relationship {"));
        assert!(output.contains("from = \"orders.customer_id\""));
        assert!(output.contains("to = \"customers.customer_id\""));
        assert!(output.contains("cardinality = \"many_to_one\""));
    }

    #[test]
    fn test_emit_relationship_long_form_non_many_to_one() {
        let rel = Relationship::new(
            "customers",
            "orders",
            "customer_id",
            "customer_id",
            Cardinality::OneToMany,
        );

        let config = EmitConfig::minimal();
        let mut w = IndentWriter::new(Indent::Tabs);

        emit_relationship(&mut w, &rel, &config);
        let output = w.into_string();

        // Non-ManyToOne uses long form even without comments
        assert!(output.contains("relationship {"));
        assert!(output.contains("cardinality = \"one_to_many\""));
    }

    #[test]
    fn test_emit_relationship_fk_source() {
        let rel = Relationship::from_foreign_key(
            "orders",
            "customers",
            "customer_id",
            "customer_id",
            Cardinality::ManyToOne,
        );

        let config = EmitConfig::default();
        let mut w = IndentWriter::new(Indent::Tabs);

        emit_relationship(&mut w, &rel, &config);
        let output = w.into_string();

        assert!(output.contains("-- Foreign key constraint"));
    }

    #[test]
    fn test_emit_relationship_inferred_source() {
        let rel = Relationship::inferred(
            "orders",
            "users",
            "user_id",
            "id",
            Cardinality::ManyToOne,
            "column_to_pk_match",
            0.85,
        );

        let config = EmitConfig::default();
        let mut w = IndentWriter::new(Indent::Tabs);

        emit_relationship(&mut w, &rel, &config);
        let output = w.into_string();

        assert!(output.contains("-- Inferred (confidence: 0.85, rule: column_to_pk_match)"));
    }

    #[test]
    fn test_emit_relationships_grouped() {
        let rels = vec![
            Relationship::from_foreign_key(
                "orders",
                "customers",
                "customer_id",
                "customer_id",
                Cardinality::ManyToOne,
            ),
            Relationship::inferred(
                "orders",
                "users",
                "user_id",
                "id",
                Cardinality::ManyToOne,
                "column_to_pk_match",
                0.85,
            ),
        ];
        let rel_refs: Vec<&Relationship> = rels.iter().collect();

        let config = EmitConfig::default();
        let mut w = IndentWriter::new(Indent::Tabs);

        emit_relationships(&mut w, &rel_refs, &config);
        let output = w.into_string();

        // Check grouping order: FK first, then inferred
        let fk_pos = output.find("Foreign key constraints").unwrap();
        let inferred_pos = output.find("Inferred relationships").unwrap();
        assert!(fk_pos < inferred_pos);
    }

    #[test]
    fn test_emit_relationships_no_comments() {
        let rels = vec![
            Relationship::new(
                "orders",
                "customers",
                "customer_id",
                "customer_id",
                Cardinality::ManyToOne,
            ),
            Relationship::new(
                "orders",
                "products",
                "product_id",
                "product_id",
                Cardinality::ManyToOne,
            ),
        ];
        let rel_refs: Vec<&Relationship> = rels.iter().collect();

        let config = EmitConfig::minimal();
        let mut w = IndentWriter::new(Indent::Tabs);

        emit_relationships(&mut w, &rel_refs, &config);
        let output = w.into_string();

        // Both should use short form
        assert!(output.contains("link(orders.customer_id, customers.customer_id)"));
        assert!(output.contains("link(orders.product_id, products.product_id)"));
        assert!(!output.contains("-- "));
    }
}
