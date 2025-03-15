package ssql

import (
	"context"
	"database/sql"
	"reflect"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"
)

// デバッグ用
// SQLを出力する
var DebugSQL = false

func First[M any](tx HasQuery, mp *M, whereClauses []string, whereValues []any) (*M, error) {
	sql, values := getQuerySQL(mp, whereClauses, whereValues)
	debugSQL(sql, values)
	return QueryFirst(tx, mp, sql, values...)
}

func Find[M any](tx HasQuery, mp *M, whereClauses []string, whereValues []any) ([]M, error) {
	sql, values := getQuerySQL(mp, whereClauses, whereValues)
	debugSQL(sql, values)
	return Query(tx, mp, sql, values...)
}

func getQuerySQL(s any, whereClauses []string, whereValues []any) (string, []any) {
	rv := checkAndGetStructValue(s)
	rt := rv.Type()

	values := []any{}
	values = append(values, whereValues...)

	whereClause := ""
	if len(whereClauses) > 0 {
		whereClause = " WHERE " + strings.Join(whereClauses, " AND ")
	}
	tableName := toTableName(rt.Name())
	query := "SELECT * FROM " + tableName + whereClause

	// Replace placeholders with $1, $2, ...
	query = replacePlaceholders(query, 0)

	return query, values
}

func replacePlaceholders(query string, startIdx int) string {
	re := regexp.MustCompile(`\?`)
	idx := startIdx
	return re.ReplaceAllStringFunc(query, func(_ string) string {
		idx++
		return "$" + strconv.Itoa(idx)
	})
}

// id, created_at, updated_atはsetFieldsに含めてもスキップされる。
// updated_atは暗黙的に更新される。
// valueを"NOW"にすると現在時刻が入る。（updated_atと同じ値が入る）
func Update(tx HasExec, s any, whereClauses []string, whereValues []any, setFields map[string]any) (sql.Result, error) {
	sql, values := getUpdateSQL(s, whereClauses, whereValues, setFields)
	debugSQL(sql, values)
	return Exec(tx, sql, values...)
}

// マップはループで順番が保障されないため、順番を保証するためにキーを取得する
func getOrderedFieldsKey(s map[string]any) []string {
	keys := make([]string, 0, len(s))
	for key := range s {
		keys = append(keys, key)
	}
	slices.Sort(keys)
	return keys
}

func getUpdateSQL(s any, whereClauses []string, whereValues []any, setFields map[string]any) (string, []any) {
	rv := checkAndGetStructValue(s)
	rt := rv.Type()

	now := time.Now()
	setClauses := []string{}
	values := []any{}

	setFieldKeys := getOrderedFieldsKey(setFields)
	for _, field := range setFieldKeys {
		// Skip id, created_at, updated_at fields
		if slices.Contains([]string{"id", "created_at", "updated_at"}, field) {
			continue
		}
		setClauses = append(setClauses, field+" = ?")

		if strVal, ok := setFields[field].(string); ok && strVal == "NOW" {
			values = append(values, now)
		} else {
			values = append(values, setFields[field])
		}
	}

	// Add updated_at field
	setClauses = append(setClauses, "updated_at = ?")
	values = append(values, now)
	values = append(values, whereValues...)

	whereClause := ""
	if len(whereClauses) > 0 {
		whereClause = " WHERE " + strings.Join(whereClauses, " AND ")
	}
	tableName := toTableName(rt.Name())
	query := "UPDATE " + tableName + " SET " + strings.Join(setClauses, ", ") + whereClause

	// Replace placeholders with $1, $2, ...
	query = replacePlaceholders(query, 0)

	return query, values
}

func Delete(tx HasExec, s any, whereClauses []string, whereValues []any) (sql.Result, error) {
	sql := getDeleteSQL(s, whereClauses)
	debugSQL(sql, whereValues)
	return Exec(tx, sql, whereValues...)
}

func getDeleteSQL(s any, whereClauses []string) string {
	rv := checkAndGetStructValue(s)
	rt := rv.Type()

	whereClause := ""
	if len(whereClauses) > 0 {
		whereClause = " WHERE " + strings.Join(whereClauses, " AND ")
	}
	tableName := toTableName(rt.Name())
	query := "DELETE FROM " + tableName + whereClause

	// Replace placeholders with $1, $2, ...
	query = replacePlaceholders(query, 0)

	return query
}

// id, created_at, updated_atには値はセットされず、データベース側のデフォルト値に委ねる。
func Insert(tx HasExec, s any) (sql.Result, error) {
	sql, values := getInsertSQL(s)
	debugSQL(sql, values)
	return Exec(tx, sql, values...)
}

func getInsertSQL(s any) (string, []any) {
	rv := checkAndGetStructValue(s)
	rt := rv.Type()

	fields := []string{}
	values := []any{}

	for i := range rt.NumField() {
		fieldName := rt.Field(i).Tag.Get("database")
		if slices.Contains([]string{"id", "created_at", "updated_at"}, fieldName) {
			continue
		}

		fields = append(fields, `"`+fieldName+`"`)

		if rv.Field(i).Kind() == reflect.Ptr {
			if rv.Field(i).IsNil() {
				values = append(values, nil)
			} else {
				values = append(values, rv.Field(i).Elem().Interface())
			}
		} else {
			values = append(values, rv.Field(i).Interface())
		}
	}

	tableName := toTableName(rt.Name())

	query := "INSERT INTO " + tableName + " (" + strings.Join(fields, ", ") + ") VALUES ("
	placeholders := []string{}
	for i := range values {
		placeholders = append(placeholders, "$"+strconv.Itoa(i+1))
	}
	query += strings.Join(placeholders, ", ") + ")"

	return query, values
}

// toTableName converts a CamelCase string to snake_case.
func toTableName(str string) string {
	re := regexp.MustCompile("([a-z0-9])([A-Z])")
	snake := re.ReplaceAllString(str, "${1}_${2}")
	return strings.ToLower(snake) + "s" // Add 's' for plural form
}

func checkAndGetStructValue(s any) reflect.Value {
	rv := reflect.ValueOf(s)
	if rv.Kind() == reflect.Ptr {
		rv = rv.Elem()
	}

	if rv.Kind() != reflect.Struct {
		panic("must be a struct")
	}
	return rv
}

func debugSQL(sql string, values []any) {
	if DebugSQL {
		l.Debug(context.Background(), sql, values)
	}
}
