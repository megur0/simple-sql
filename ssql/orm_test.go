package ssql

import (
	"reflect"
	"testing"

	"github.com/megur0/testutil"
)

type TestStruct struct {
	ID        int    `database:"id"`
	Name      string `database:"name"`
	Age       int    `database:"age"`
	CreatedAt string `database:"created_at"`
	UpdatedAt string `database:"updated_at"`
}

type TestStructWithMap struct {
	Data map[string]string `database:"data"`
}

// env `cat .env` go test -v -count=1 -timeout 60s -run ^TestGetInsertSQL$ ./ssql
func TestGetInsertSQL(t *testing.T) {
	tests := []struct {
		name         string
		input        any
		expected     string
		expectedVals []any
	}{
		{
			name:         "valid struct",
			input:        TestStruct{ID: 1, Name: "John", Age: 30},
			expected:     `INSERT INTO test_structs ("name", "age") VALUES ($1, $2)`,
			expectedVals: []any{"John", 30},
		},
		{
			name:         "invalid input (non-struct)",
			input:        123,
			expected:     "panic",
			expectedVals: nil,
		},
		{
			name:         "valid struct with CreatedAt",
			input:        TestStruct{ID: 2, Name: "Jane", Age: 25, CreatedAt: "2023-10-01"},
			expected:     `INSERT INTO test_structs ("name", "age") VALUES ($1, $2)`,
			expectedVals: []any{"Jane", 25},
		},
		{
			name:         "valid struct with UpdatedAt",
			input:        TestStruct{ID: 3, Name: "Doe", Age: 40, UpdatedAt: "2023-10-02"},
			expected:     `INSERT INTO test_structs ("name", "age") VALUES ($1, $2)`,
			expectedVals: []any{"Doe", 40},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			defer func() {
				if r := recover(); r != nil {
					if tt.expected != "panic" {
						t.Errorf("expected no panic, but got panic: %v", r)
					}
				}
			}()

			var sql string
			var values []any
			switch v := tt.input.(type) {
			case TestStruct:
				sql, values = getInsertSQL(v)
			case int:
				sql, values = getInsertSQL(v)
			case TestStructWithMap:
				sql, values = getInsertSQL(v)
			default:
			}

			if sql != tt.expected && tt.expected != "panic" {
				t.Errorf("expected %v, got %v", tt.expected, sql)
			}

			if !reflect.DeepEqual(values, tt.expectedVals) {
				t.Errorf("expected %v, got %v", tt.expectedVals, values)
			}
		})
	}
}

// env `cat .env` go test -v -count=1 -timeout 60s -run ^TestToTableName$ ./ssql
func TestToTableName(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"TestStruct", "test_structs"},
		{"User", "users"},
		{"OrderItem", "order_items"},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			result := toTableName(tt.input)
			if result != tt.expected {
				t.Errorf("expected %v, got %v", tt.expected, result)
			}
		})
	}
}

// env `cat .env` go test -v -count=1 -timeout 60s -run ^TestGetQuerySQL$ ./ssql
func TestGetQuerySQL(t *testing.T) {
	tests := []struct {
		name         string
		input        any
		whereClauses []string
		whereValues  []any
		expected     string
	}{
		{
			name:     "simple struct",
			input:    TestStruct{},
			expected: "SELECT * FROM test_structs",
		},
		{
			name:         "struct with where clause",
			input:        TestStruct{},
			whereClauses: []string{"name = ?", "age = ?"},
			whereValues:  []any{"John", 30},
			expected:     "SELECT * FROM test_structs WHERE name = $1 AND age = $2",
		},
		{
			name:         "struct with where clause",
			input:        TestStruct{},
			whereClauses: []string{"name = ?", "is_valid = true"},
			whereValues:  []any{"John"},
			expected:     "SELECT * FROM test_structs WHERE name = $1 AND is_valid = true",
		},
		{
			name:     "struct with map",
			input:    TestStructWithMap{Data: map[string]string{"key": "value"}},
			expected: "SELECT * FROM test_struct_with_maps",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sql, _ := getQuerySQL(tt.input, tt.whereClauses, tt.whereValues)

			if sql != tt.expected {
				t.Errorf("expected %v, got %v", tt.expected, sql)
			}
		})
	}
}

// env `cat .env` go test -v -count=1 -timeout 60s -run ^TestGetUpdateSQL$ ./ssql
func TestGetUpdateSQL(t *testing.T) {
	tests := []struct {
		name         string
		input        any
		whereClauses []string
		whereValues  []any
		setClauses   []string
		setValues    []any
		expected     string
	}{
		{
			name:         "simple struct",
			input:        TestStruct{},
			whereClauses: []string{"id = ?"},
			whereValues:  []any{1},
			setClauses:   []string{"name = ?", "age = ?"},
			setValues:    []any{"John", 30, "2023-10-01"},
			expected:     "UPDATE test_structs SET name = $1, age = $2, updated_at = $3 WHERE id = $4",
		},
		{
			name:         "struct with where clause",
			input:        TestStruct{},
			whereClauses: []string{"name = ?", "age = ?"},
			whereValues:  []any{"John", 30},
			setClauses:   []string{"name = ?", "age = ?"},
			setValues:    []any{"John", 30, "2023-10-01"},
			expected:     "UPDATE test_structs SET name = $1, age = $2, updated_at = $3 WHERE name = $4 AND age = $5",
		},
		{
			name:       "struct with map",
			input:      TestStructWithMap{},
			setClauses: []string{"data = ?"},
			setValues:  []any{map[string]string{"data": "value"}, "2023-10-01"},
			expected:   "UPDATE test_struct_with_maps SET data = $1, updated_at = $2",
		},
		{
			name:         "struct with complex set clause",
			input:        TestStruct{},
			whereClauses: []string{"id = ?"},
			whereValues:  []any{1},
			setClauses:   []string{"age = (age + 1)"},
			setValues:    []any{"2023-10-01"},
			expected:     "UPDATE test_structs SET age = (age + 1), updated_at = $1 WHERE id = $2",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sql, _ := getUpdateSQL(tt.input, tt.whereClauses, tt.whereValues, tt.setClauses, tt.setValues)

			if sql != tt.expected {
				t.Errorf("expected %v, got %v", tt.expected, sql)
			}
		})
	}
}

// env `cat .env` go test -v -count=1 -timeout 60s -run ^TestGetDeleteSQL$ ./ssql
func TestGetDeleteSQL(t *testing.T) {
	tests := []struct {
		name         string
		input        any
		whereClauses []string
		whereValues  []any
		expectedSQL  string
	}{
		{
			name:         "simple struct",
			input:        TestStruct{ID: 1, Name: "John", Age: 30},
			whereClauses: []string{"id = ?"},
			whereValues:  []any{1},
			expectedSQL:  "DELETE FROM test_structs WHERE id = $1",
		},
		{
			name:         "struct with multiple where clauses",
			input:        TestStruct{ID: 1, Name: "John", Age: 30},
			whereClauses: []string{"name = ?", "age = ?"},
			whereValues:  []any{"John", 30},
			expectedSQL:  "DELETE FROM test_structs WHERE name = $1 AND age = $2",
		},
		{
			name:         "struct with map",
			input:        TestStructWithMap{Data: map[string]string{"key": "value"}},
			whereClauses: []string{"data = ?"},
			whereValues:  []any{map[string]string{"key": "value"}},
			expectedSQL:  "DELETE FROM test_struct_with_maps WHERE data = $1",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sql := getDeleteSQL(tt.input, tt.whereClauses)

			if sql != tt.expectedSQL {
				t.Errorf("expected %v, got %v", tt.expectedSQL, sql)
			}
		})
	}
}

// env `cat .env` go test -v -count=1 -timeout 60s -run ^TestORM$ ./ssql
func TestORM(t *testing.T) {
	refreshDB()

	t.Run("success_insert", func(t *testing.T) {
		result, err := Insert(nil, TableForTest{Name: Ptr("aaaaaa"), UID: "aaa"})
		if err != nil {
			t.Fatal("got error")
		}
		id, _ := result.LastInsertId()
		row, _ := result.RowsAffected()
		df("%v, %v", id, row)
		testutil.AssertEqual(t, int(row), 1)
	})

	t.Run("success_find", func(t *testing.T) {
		a, err := Find(nil, &TableForTest{}, []string{"uid = Any(?)"}, []any{[]string{"ccc", "ddd", "eee"}})
		if err != nil {
			t.Error("got error")
		}
		if len(a) != 0 {
			t.Errorf("expected 0, got %v", len(a))
		}

		b, err := Find(nil, &TableForTest{}, []string{"uid = Any(?)"}, []any{[]string{"aaa", "bbb"}})
		if err != nil {
			t.Error("got error")
		}
		if len(b) != 1 {
			t.Errorf("expected 1, got %v", len(a))
		}
	})

	t.Run("success_first", func(t *testing.T) {
		r, err := First(nil, &TableForTest{}, []string{"uid = ?"}, []any{"aaa"})
		if err != nil {
			t.Fatal("got error")
		}
		if r == nil {
			t.Error("result should not be nil")
		}
		r, err = First(nil, &TableForTest{}, []string{"uid = ?"}, []any{"bbb"})
		if err != nil {
			t.Fatal("got error")
		}
		if r != nil {
			t.Error("result should be nil")
		}
	})

	t.Run("success_update", func(t *testing.T) {
		result, err := Update(nil, &TableForTest{}, []string{"uid = ?"}, []any{"aaa"}, map[string]any{"name": "bbbbbb"})
		if err != nil {
			t.Fatal("got error")
		}
		id, _ := result.LastInsertId()
		row, _ := result.RowsAffected()
		df("%v, %v", id, row)
		testutil.AssertEqual(t, int(row), 1)
	})
}
