package driver

import (
	"strings"
	"testing"
)

func TestValidateIdentifier(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		wantErr bool
	}{
		// Valid identifiers
		{"simple", "users", false},
		{"with_underscore", "user_accounts", false},
		{"starts_underscore", "_private", false},
		{"mixed_case", "UserAccounts", false},
		{"with_numbers", "table123", false},
		{"underscore_numbers", "tbl_2024_data", false},
		{"single_char", "a", false},
		{"max_length", strings.Repeat("a", 128), false},

		// Invalid identifiers
		{"empty", "", true},
		{"starts_number", "123table", true},
		{"has_space", "user accounts", true},
		{"has_dash", "user-accounts", true},
		{"has_dot", "schema.table", true},
		{"has_semicolon", "users;DROP", true},
		{"has_quotes", `users"`, true},
		{"has_brackets", "users]", true},
		{"sql_injection_attempt", "users]; DROP TABLE users--", true},
		{"too_long", string(make([]byte, 200)), true},
		{"just_over_max", strings.Repeat("a", 129), true},

		// SQL injection patterns
		{"union_select", "users UNION SELECT", true},
		{"comment_dash", "users--comment", true},
		{"comment_hash", "users#comment", true},
		{"or_injection", "users OR 1=1", true},
		{"and_injection", "users AND 1=1", true},
		{"null_byte", "users\x00DROP", true},
		{"newline", "users\nDROP", true},
		{"tab", "users\tDROP", true},
		{"backtick", "users`DROP", true},
		{"single_quote", "users'DROP", true},
		{"double_quote", `users"DROP`, true},
		{"backslash", "users\\DROP", true},
		{"forward_slash", "users/DROP", true},
		{"asterisk", "users*", true},
		{"percent", "users%", true},
		{"at_sign", "users@host", true},
		{"exclamation", "users!", true},
		{"equals", "users=1", true},
		{"less_than", "users<1", true},
		{"greater_than", "users>1", true},
		{"pipe", "users|cat", true},
		{"ampersand", "users&cmd", true},
		{"dollar", "users$var", true},
		{"caret", "users^", true},
		{"parenthesis", "users()", true},
		{"curly_brace", "users{}", true},
		{"square_bracket", "users[]", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateIdentifier(tt.input)
			if tt.wantErr && err == nil {
				t.Errorf("ValidateIdentifier(%q) should return error", tt.input)
			}
			if !tt.wantErr && err != nil {
				t.Errorf("ValidateIdentifier(%q) unexpected error: %v", tt.input, err)
			}
		})
	}
}

func TestValidateSchemaTable(t *testing.T) {
	tests := []struct {
		name    string
		schema  string
		table   string
		wantErr bool
	}{
		{"valid", "dbo", "users", false},
		{"valid_with_underscore", "my_schema", "user_accounts", false},
		{"invalid_schema", "dbo;DROP", "users", true},
		{"invalid_table", "dbo", "users;DROP", true},
		{"both_invalid", "bad;", "bad;", true},
		{"empty_schema", "", "users", true},
		{"empty_table", "dbo", "", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateSchemaTable(tt.schema, tt.table)
			if tt.wantErr && err == nil {
				t.Errorf("ValidateSchemaTable(%q, %q) should return error", tt.schema, tt.table)
			}
			if !tt.wantErr && err != nil {
				t.Errorf("ValidateSchemaTable(%q, %q) unexpected error: %v", tt.schema, tt.table, err)
			}
		})
	}
}
