//! textDocument/signatureHelp handler

use tower_lsp::lsp_types::{
    Documentation, ParameterInformation, ParameterLabel, Position, SignatureHelp,
    SignatureInformation,
};

use crate::lsp::analysis::document::DocumentState;

/// Function signatures for DSL functions.
struct FunctionSignature {
    name: &'static str,
    label: &'static str,
    doc: &'static str,
    params: &'static [(&'static str, &'static str)], // (name, description)
}

const SIGNATURES: &[FunctionSignature] = &[
    FunctionSignature {
        name: "decimal",
        label: "decimal(precision, scale)",
        doc: "Decimal number with fixed precision and scale",
        params: &[
            ("precision", "Total number of digits (1-38)"),
            ("scale", "Digits after decimal point"),
        ],
    },
    FunctionSignature {
        name: "varchar",
        label: "varchar(length)",
        doc: "Variable-length string with maximum length",
        params: &[("length", "Maximum character length")],
    },
    FunctionSignature {
        name: "between",
        label: "between(column, min, max)",
        doc: "Filter column between min and max values",
        params: &[
            ("column", "Column reference (entity.column)"),
            ("min", "Minimum value (inclusive)"),
            ("max", "Maximum value (inclusive)"),
        ],
    },
    FunctionSignature {
        name: "filter",
        label: "filter(column, op, value)",
        doc: "Filter condition on a column",
        params: &[
            ("column", "Column reference (entity.column)"),
            ("op", "Operator: =, !=, <, >, <=, >=, like"),
            ("value", "Value to compare against"),
        ],
    },
    FunctionSignature {
        name: "rolling_sum",
        label: "rolling_sum(measure, periods)",
        doc: "Rolling sum over N periods",
        params: &[
            ("measure", "Measure expression"),
            ("periods", "Number of periods to sum"),
        ],
    },
    FunctionSignature {
        name: "rolling_avg",
        label: "rolling_avg(measure, periods)",
        doc: "Rolling average over N periods",
        params: &[
            ("measure", "Measure expression"),
            ("periods", "Number of periods to average"),
        ],
    },
];

/// Get signature help at position.
pub fn get_signature_help(doc: &DocumentState, position: Position) -> Option<SignatureHelp> {
    let node = doc.node_at_position(position)?;

    // Walk up to find function_call
    let mut current = Some(node);
    let mut args_node = None;

    while let Some(n) = current {
        if n.kind() == "arguments" {
            args_node = Some(n);
        }
        if n.kind() == "function_call" {
            return build_signature_help(n, args_node, &doc.source, position);
        }
        current = n.parent();
    }

    None
}

fn build_signature_help(
    call: tree_sitter::Node,
    args_node: Option<tree_sitter::Node>,
    source: &str,
    position: Position,
) -> Option<SignatureHelp> {
    // Get function name
    let first = call.child(0)?;
    if first.kind() != "identifier" {
        return None;
    }
    let func_name = &source[first.start_byte()..first.end_byte()];

    // Find matching signature
    let sig = SIGNATURES.iter().find(|s| s.name == func_name)?;

    // Count commas before cursor to determine active parameter
    let active_param = if let Some(args) = args_node {
        count_params_before_position(args, source, position)
    } else {
        0
    };

    let parameters: Vec<ParameterInformation> = sig
        .params
        .iter()
        .map(|(name, doc)| ParameterInformation {
            label: ParameterLabel::Simple(name.to_string()),
            documentation: Some(Documentation::String(doc.to_string())),
        })
        .collect();

    Some(SignatureHelp {
        signatures: vec![SignatureInformation {
            label: sig.label.to_string(),
            documentation: Some(Documentation::String(sig.doc.to_string())),
            parameters: Some(parameters),
            active_parameter: Some(active_param),
        }],
        active_signature: Some(0),
        active_parameter: Some(active_param),
    })
}

fn count_params_before_position(args: tree_sitter::Node, source: &str, position: Position) -> u32 {
    let args_text = &source[args.start_byte()..args.end_byte()];
    let cursor_col = position.character as usize;
    let args_start_col = args.start_position().column;

    if cursor_col <= args_start_col {
        return 0;
    }

    let cursor_offset = cursor_col - args_start_col;

    // Count commas before cursor position
    args_text[..cursor_offset.min(args_text.len())]
        .chars()
        .filter(|&c| c == ',')
        .count() as u32
}

#[cfg(test)]
mod tests {
    use super::*;
    use tower_lsp::lsp_types::Url;

    #[test]
    fn test_signature_help_decimal_first_param() {
        let source = r#"amount = decimal(10)"#;
        let doc = DocumentState::new(
            Url::parse("file:///test.lua").unwrap(),
            1,
            source.to_string(),
        );

        let position = Position {
            line: 0,
            character: 18,
        };
        let result = get_signature_help(&doc, position);

        assert!(result.is_some());
        let help = result.unwrap();
        assert_eq!(help.signatures[0].label, "decimal(precision, scale)");
        assert_eq!(help.active_parameter, Some(0));
    }

    #[test]
    fn test_signature_help_decimal_second_param() {
        let source = r#"amount = decimal(10, 2)"#;
        let doc = DocumentState::new(
            Url::parse("file:///test.lua").unwrap(),
            1,
            source.to_string(),
        );

        // Position after the comma
        let position = Position {
            line: 0,
            character: 21,
        };
        let result = get_signature_help(&doc, position);

        assert!(result.is_some());
        let help = result.unwrap();
        assert_eq!(help.active_parameter, Some(1));
    }
}
