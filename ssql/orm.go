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
	sql, values := getQuerySQL(mp, whereClauses, whereValues, nil, nil)
	debugSQL(sql, values)
	return QueryFirst(tx, mp, sql, values...)
}

func FirstLimit[M any](tx HasQuery, mp *M, whereClauses []string, whereValues []any, orderByClauses []string, limitOffset map[string]int) (*M, error) {
	sql, values := getQuerySQL(mp, whereClauses, whereValues, orderByClauses, limitOffset)
	debugSQL(sql, values)
	return QueryFirst(tx, mp, sql, values...)
}

func Find[M any](tx HasQuery, mp *M, whereClauses []string, whereValues []any) ([]M, error) {
	sql, values := getQuerySQL(mp, whereClauses, whereValues, nil, nil)
	debugSQL(sql, values)
	return Query(tx, mp, sql, values...)
}

// OrderBy, Limit, Offsetを指定する場合
// limitOffsetはmapで"limit"と"offset"を指定する。
func FindLimit[M any](tx HasQuery, mp *M, whereClauses []string, whereValues []any, orderByClauses []string, limitOffset map[string]int) ([]M, error) {
	sql, values := getQuerySQL(mp, whereClauses, whereValues, orderByClauses, limitOffset)
	debugSQL(sql, values)
	return Query(tx, mp, sql, values...)
}

func getQuerySQL(s any, whereClauses []string, whereValues []any, orderByClauses []string, limitOffset map[string]int) (string, []any) {
	rv := checkAndGetStructValue(s)
	rt := rv.Type()

	values := []any{}
	values = append(values, whereValues...)

	whereClause := ""
	if len(whereClauses) > 0 {
		whereClause = " WHERE " + strings.Join(whereClauses, " AND ")
	}
	orderByClause := ""
	if len(orderByClauses) > 0 {
		orderByClause = " ORDER BY " + strings.Join(orderByClauses, ", ")
	}
	limitClause := ""
	offsetClause := ""
	if limitOffset != nil {
		if limit, ok := limitOffset["limit"]; ok {
			limitClause = " LIMIT ?"
			values = append(values, limit)
		}

		if offset, ok := limitOffset["offset"]; ok {
			offsetClause = " OFFSET ?"
			values = append(values, offset)
		}
	}

	tableName := toTableName(rt.Name())
	query := "SELECT * FROM " + tableName + whereClause + orderByClause + limitClause + offsetClause

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

// updated_atは暗黙的に更新される。
// valueを"NOW"にすると現在時刻が入る。（updated_atと同じ値が入る）
func Update(tx HasExec, s any, whereClauses []string, whereValues []any, setMaps map[string]any) (sql.Result, error) {
	setClauses := []string{}
	setValues := []any{}
	setField := getOrderedKeys(setMaps)
	for _, field := range setField {
		setClauses = append(setClauses, field+" = ?")
		setValues = append(setValues, setMaps[field])
	}
	sql, setValues := getUpdateSQL(s, whereClauses, whereValues, setClauses, setValues)
	debugSQL(sql, setValues)
	return Exec(tx, sql, setValues...)
}

// Updateするフィールドに式を指定したい場合に利用する
func UpdateWithClauses(tx HasExec, s any, whereClauses []string, whereValues []any, setClauses []string, setValues []any) (sql.Result, error) {
	sql, values := getUpdateSQL(s, whereClauses, whereValues, setClauses, setValues)
	debugSQL(sql, values)
	return Exec(tx, sql, values...)
}

// マップはループで順番が保障されないため、順番を保証するためにキーを取得する
func getOrderedKeys(s map[string]any) []string {
	keys := make([]string, 0, len(s))
	for key := range s {
		keys = append(keys, key)
	}
	slices.Sort(keys)
	return keys
}

func getUpdateSQL(s any, whereClauses []string, whereValues []any, setClauses []string, setValues []any) (string, []any) {
	rv := checkAndGetStructValue(s)
	rt := rv.Type()

	now := time.Now()
	setClauses2 := slices.Clone(setClauses)
	values := slices.Clone(setValues)

	for i, setValue := range setValues {
		if strVal, ok := setValue.(string); ok && strVal == "NOW" {
			values[i] = now
		}
	}

	setClauses2 = append(setClauses2, "updated_at = ?")
	values = append(values, now)
	values = append(values, whereValues...)

	whereClause := ""
	if len(whereClauses) > 0 {
		whereClause = " WHERE " + strings.Join(whereClauses, " AND ")
	}
	tableName := toTableName(rt.Name())
	query := "UPDATE " + tableName + " SET " + strings.Join(setClauses2, ", ") + whereClause

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
	sql, values := getInsertSQL(s, []string{"id", "created_at", "updated_at"})
	debugSQL(sql, values)
	return Exec(tx, sql, values...)
}

// 複数のデータを一度に挿入する。
// id, created_at, updated_atには値はセットされず、データベース側のデフォルト値に委ねる。
func InsertBulk[T any](tx HasExec, items []T) (sql.Result, error) {
	if len(items) == 0 {
		return nil, nil
	}
	sql, values := getBulkInsertSQL(items, []string{"id", "created_at", "updated_at"})
	debugSQL(sql, values)
	return Exec(tx, sql, values...)
}

// セットしないフィールドを明示的に指定する。
func InsertWithIgnores(tx HasExec, s any, ignores []string) (sql.Result, error) {
	sql, values := getInsertSQL(s, ignores)
	debugSQL(sql, values)
	return Exec(tx, sql, values...)
}

// 複数のデータを一度に挿入する。セットしないフィールドを明示的に指定する。
func InsertBulkWithIgnores[T any](tx HasExec, items []T, ignores []string) (sql.Result, error) {
	if len(items) == 0 {
		return nil, nil
	}
	sql, values := getBulkInsertSQL(items, ignores)
	debugSQL(sql, values)
	return Exec(tx, sql, values...)
}

// 複数のデータを一括挿入するためのSQLを生成する
func getBulkInsertSQL[T any](items []T, ignores []string) (string, []any) {
	if len(items) == 0 {
		return "", nil
	}

	// 最初の要素から構造体の型情報を取得
	item0 := items[0]
	rv := checkAndGetStructValue(item0)
	rt := rv.Type()

	// フィールド情報を取得
	fields := []string{}
	fieldIndices := []int{}

	for i := 0; i < rt.NumField(); i++ {
		fieldName := rt.Field(i).Tag.Get("database")
		if slices.Contains(ignores, fieldName) {
			continue
		}

		fields = append(fields, `"`+fieldName+`"`)
		fieldIndices = append(fieldIndices, i)
	}

	// テーブル名を取得
	tableName := toTableName(rt.Name())

	// カラム部分の生成
	query := "INSERT INTO " + tableName + " (" + strings.Join(fields, ", ") + ") VALUES "

	// 値部分の生成
	valueGroups := []string{}
	values := []any{}
	paramCount := 1

	for _, item := range items {
		rv := checkAndGetStructValue(item)

		placeholders := []string{}
		for _, idx := range fieldIndices {
			placeholders = append(placeholders, "$"+strconv.Itoa(paramCount))
			paramCount++

			if rv.Field(idx).Kind() == reflect.Ptr {
				if rv.Field(idx).IsNil() {
					values = append(values, nil)
				} else {
					values = append(values, rv.Field(idx).Elem().Interface())
				}
			} else {
				values = append(values, rv.Field(idx).Interface())
			}
		}

		valueGroups = append(valueGroups, "("+strings.Join(placeholders, ", ")+")")
	}

	query += strings.Join(valueGroups, ", ")

	return query, values
}

func getInsertSQL(s any, ignores []string) (string, []any) {
	rv := checkAndGetStructValue(s)
	rt := rv.Type()

	fields := []string{}
	values := []any{}

	for i := range rt.NumField() {
		fieldName := rt.Field(i).Tag.Get("database")
		if slices.Contains(ignores, fieldName) {
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
