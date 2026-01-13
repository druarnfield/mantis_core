//! Shared string inflection utilities.
//!
//! Provides pluralization and singularization for table/column name matching.
//! Uses the `inflector` crate with additional handling for common irregular plurals
//! that appear in database schemas.

use inflector::Inflector;

/// Known irregular plurals that inflector doesn't handle well for database contexts.
static IRREGULAR_PLURALS: &[(&str, &str)] = &[
    // People
    ("person", "people"),
    ("child", "children"),
    ("man", "men"),
    ("woman", "women"),
    // Body parts
    ("foot", "feet"),
    ("tooth", "teeth"),
    // Animals
    ("goose", "geese"),
    ("mouse", "mice"),
    ("ox", "oxen"),
    // -f/-fe → -ves
    ("leaf", "leaves"),
    ("life", "lives"),
    ("knife", "knives"),
    ("wife", "wives"),
    ("half", "halves"),
    ("self", "selves"),
    ("calf", "calves"),
    ("loaf", "loaves"),
    // -o → -oes
    ("potato", "potatoes"),
    ("tomato", "tomatoes"),
    ("hero", "heroes"),
    // Latin/Greek
    ("analysis", "analyses"),
    ("basis", "bases"),
    ("crisis", "crises"),
    ("diagnosis", "diagnoses"),
    ("hypothesis", "hypotheses"),
    ("thesis", "theses"),
    ("phenomenon", "phenomena"),
    ("criterion", "criteria"),
    ("datum", "data"),
    ("medium", "media"),
    ("index", "indices"),
    ("appendix", "appendices"),
    ("matrix", "matrices"),
    ("vertex", "vertices"),
];

/// Pluralize a word, handling irregulars first then falling back to inflector.
///
/// # Examples
/// ```ignore
/// assert_eq!(pluralize("customer"), "customers");
/// assert_eq!(pluralize("category"), "categories");
/// assert_eq!(pluralize("person"), "people");
/// assert_eq!(pluralize("analysis"), "analyses");
/// ```
pub fn pluralize(word: &str) -> String {
    if word.is_empty() {
        return String::new();
    }

    let lower = word.to_lowercase();

    // Check irregular plurals first
    for (singular, plural) in IRREGULAR_PLURALS {
        if lower == *singular {
            return plural.to_string();
        }
        // Already plural?
        if lower == *plural {
            return plural.to_string();
        }
    }

    // Fall back to inflector for regular words
    word.to_plural()
}

/// Singularize a word, handling irregulars first then falling back to inflector.
///
/// # Examples
/// ```ignore
/// assert_eq!(singularize("customers"), "customer");
/// assert_eq!(singularize("categories"), "category");
/// assert_eq!(singularize("people"), "person");
/// assert_eq!(singularize("analyses"), "analysis");
/// ```
pub fn singularize(word: &str) -> String {
    if word.is_empty() {
        return String::new();
    }

    let lower = word.to_lowercase();

    // Check irregular plurals first
    for (singular, plural) in IRREGULAR_PLURALS {
        if lower == *plural {
            return singular.to_string();
        }
        // Already singular?
        if lower == *singular {
            return singular.to_string();
        }
    }

    // Fall back to inflector for regular words
    word.to_singular()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pluralize_regular() {
        assert_eq!(pluralize("customer"), "customers");
        assert_eq!(pluralize("order"), "orders");
        assert_eq!(pluralize("product"), "products");
        assert_eq!(pluralize("user"), "users");
    }

    #[test]
    fn test_pluralize_y_ending() {
        assert_eq!(pluralize("category"), "categories");
        assert_eq!(pluralize("company"), "companies");
        assert_eq!(pluralize("country"), "countries");
        // Words ending in vowel + y just add s
        assert_eq!(pluralize("key"), "keys");
        assert_eq!(pluralize("day"), "days");
    }

    #[test]
    fn test_pluralize_s_x_ending() {
        assert_eq!(pluralize("address"), "addresses");
        assert_eq!(pluralize("box"), "boxes");
        assert_eq!(pluralize("tax"), "taxes");
    }

    #[test]
    fn test_pluralize_irregular() {
        assert_eq!(pluralize("person"), "people");
        assert_eq!(pluralize("child"), "children");
        assert_eq!(pluralize("man"), "men");
        assert_eq!(pluralize("woman"), "women");
        assert_eq!(pluralize("foot"), "feet");
        assert_eq!(pluralize("tooth"), "teeth");
        assert_eq!(pluralize("goose"), "geese");
        assert_eq!(pluralize("mouse"), "mice");
    }

    #[test]
    fn test_pluralize_latin_greek() {
        assert_eq!(pluralize("analysis"), "analyses");
        assert_eq!(pluralize("basis"), "bases");
        assert_eq!(pluralize("criterion"), "criteria");
        assert_eq!(pluralize("datum"), "data");
        assert_eq!(pluralize("index"), "indices");
        assert_eq!(pluralize("matrix"), "matrices");
    }

    #[test]
    fn test_pluralize_f_ending() {
        assert_eq!(pluralize("leaf"), "leaves");
        assert_eq!(pluralize("knife"), "knives");
        assert_eq!(pluralize("wife"), "wives");
        assert_eq!(pluralize("half"), "halves");
    }

    #[test]
    fn test_pluralize_already_plural() {
        assert_eq!(pluralize("customers"), "customers");
        assert_eq!(pluralize("people"), "people");
        assert_eq!(pluralize("data"), "data");
    }

    #[test]
    fn test_pluralize_empty() {
        assert_eq!(pluralize(""), "");
    }

    #[test]
    fn test_singularize_regular() {
        assert_eq!(singularize("customers"), "customer");
        assert_eq!(singularize("orders"), "order");
        assert_eq!(singularize("products"), "product");
        assert_eq!(singularize("users"), "user");
    }

    #[test]
    fn test_singularize_ies_ending() {
        assert_eq!(singularize("categories"), "category");
        assert_eq!(singularize("companies"), "company");
        assert_eq!(singularize("countries"), "country");
    }

    #[test]
    fn test_singularize_es_ending() {
        assert_eq!(singularize("addresses"), "address");
        assert_eq!(singularize("boxes"), "box");
        assert_eq!(singularize("taxes"), "tax");
    }

    #[test]
    fn test_singularize_irregular() {
        assert_eq!(singularize("people"), "person");
        assert_eq!(singularize("children"), "child");
        assert_eq!(singularize("men"), "man");
        assert_eq!(singularize("women"), "woman");
        assert_eq!(singularize("feet"), "foot");
        assert_eq!(singularize("teeth"), "tooth");
        assert_eq!(singularize("geese"), "goose");
        assert_eq!(singularize("mice"), "mouse");
    }

    #[test]
    fn test_singularize_latin_greek() {
        assert_eq!(singularize("analyses"), "analysis");
        assert_eq!(singularize("bases"), "basis");
        assert_eq!(singularize("criteria"), "criterion");
        assert_eq!(singularize("data"), "datum");
        assert_eq!(singularize("indices"), "index");
        assert_eq!(singularize("matrices"), "matrix");
    }

    #[test]
    fn test_singularize_ves_ending() {
        assert_eq!(singularize("leaves"), "leaf");
        assert_eq!(singularize("knives"), "knife");
        assert_eq!(singularize("wives"), "wife");
        assert_eq!(singularize("halves"), "half");
    }

    #[test]
    fn test_singularize_already_singular() {
        assert_eq!(singularize("customer"), "customer");
        assert_eq!(singularize("person"), "person");
        assert_eq!(singularize("datum"), "datum");
    }

    #[test]
    fn test_singularize_empty() {
        assert_eq!(singularize(""), "");
    }

    #[test]
    fn test_roundtrip() {
        // Pluralize then singularize should return original
        let words = ["customer", "category", "person", "analysis", "leaf"];
        for word in words {
            let plural = pluralize(word);
            let back = singularize(&plural);
            assert_eq!(back, word, "Roundtrip failed for '{}'", word);
        }
    }
}
