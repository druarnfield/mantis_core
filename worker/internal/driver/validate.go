package driver

import (
	"fmt"
	"regexp"
)

// validIdentifier matches standard SQL identifiers.
// Allows alphanumeric characters and underscores, must start with letter or underscore.
// Max length 128 characters (common SQL limit).
var validIdentifier = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]{0,127}$`)

// ValidateIdentifier checks if a string is a safe SQL identifier.
// Returns an error if the identifier contains potentially dangerous characters.
func ValidateIdentifier(name string) error {
	if name == "" {
		return fmt.Errorf("identifier cannot be empty")
	}
	if !validIdentifier.MatchString(name) {
		return fmt.Errorf("invalid identifier %q: must contain only alphanumeric characters and underscores, start with letter or underscore", name)
	}
	return nil
}

// ValidateSchemaTable validates both schema and table identifiers.
func ValidateSchemaTable(schema, table string) error {
	if err := ValidateIdentifier(schema); err != nil {
		return fmt.Errorf("invalid schema: %w", err)
	}
	if err := ValidateIdentifier(table); err != nil {
		return fmt.Errorf("invalid table: %w", err)
	}
	return nil
}
