package sql

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"

	"github.com/jackc/pgx/v5"
)

var DB *sql.DB

var Mode = MODE_DEBUG

const (
	MODE_PRODUCTION = "production"
	MODE_DEBUG      = "debug"
)

// デバッグモードの際にSQLのExpalinをチェックして"Seq Scan"を含む場合にpanicとさせる。
// これを利用することでインデックスの設定漏れを回避できる。
var useSeqScanCheck = true

// Seq Scanのチェックを個別に外したい場合は、以下のようにする。
// WHERE 'seq scan check disable'='seq scan check disable' AND (以降条件文)
const seqScanCheckDisableClause = "seq scan check disable"

// デバッグモードの際にWHEREが含まれない検索をpanicとさせる。
// これによってデータの全検索を回避する。
var useWhereCheck = true

// WHEREのチェックを個別に外したい場合は、以下のようにする。
// WHERE 'where check disable'='where check disable' AND (以降条件文)
const disableWhereCheckClause = "where check disable"

// FOR SELECTやFOR UPDATEの際はNOWAITが付与されている事を矯正する
var forceNowaitOnLockingRead = true

// UPDATE文の際は"updated_at"が含まれている事を強制する
var forceUpdatedAtCheck = true

func isDebugMode() bool {
	if Mode == MODE_PRODUCTION {
		return false

	} else if Mode == MODE_DEBUG {
		return true
	} else {
		panic("invalid Mode")
	}
}

type HasQuery interface {
	Query(query string, args ...any) (*sql.Rows, error)
}

type HasExec interface {
	Exec(query string, args ...any) (sql.Result, error)
}

func doAndRecover(c context.Context, tx *sql.Tx, f func(*sql.Tx) error) error {
	defer func() {
		if r := recover(); r != nil {
			l.Warn(c, "rollback start because panic occured")
			if err := tx.Rollback(); err != nil {
				panic(err)
			}
			l.Warn(c, "rollback end")

			// panicのスタックトレース情報を最終的に出力させたいので引き継ぐ。
			panic(r)
		}
	}()
	// ※ ここでpanicが起きた場合は 後続のreturnステートメントは実行されない。
	err := f(tx)

	return err
}

func QueryFirst[M any](tx HasQuery, mp *M, query string, args ...any) (*M, error) {
	result, err := Query(tx, mp, query, args...)
	if err != nil {
		return nil, err
	}
	if len(result) < 1 {
		return nil, nil
	}
	return &result[0], nil
}

// 取得したレコードを構造体へ格納してリストとして返す
//
// 1件もデータが存在しない場合は空の配列を返す。
// エラーの場合はnilとerrorを返す。
func Query[M any](tx HasQuery, mp *M, query string, args ...any) ([]M, error) {
	// モデルがnilだとランタイムエラーとなるため、ここでチェックする
	if mp == nil {
		panic("arg mp must not be null")
	}

	// プレースホルダー（$）とargsの個数が一致しない場合はエラーとする。
	// ※ この仕様上、同じSQL内に$xを複数回使うことはできない。
	if strings.Count(query, "$") != len(args) {
		panic(PanicPlaceHolderNumberNotMatch)
	}

	// db.Queryはselect以外を実行しても問題なく動作する。
	// 意図せず事故を起こさないように、この関数ではSELECTのみ許容する。
	if !StrContainWithIgnoreCase(query, "SELECT ") {
		panic(PanicQueryNotContanSelect)
	}

	if useWhereCheck && !StrContainWithIgnoreCase(query, " WHERE ") && !StrContainWithIgnoreCase(query, disableWhereCheckClause) {
		panic(PanicSelectSQLMustUseWhere)
	}

	if forceNowaitOnLockingRead && (StrContainWithIgnoreCase(query, " FOR SELECT") || StrContainWithIgnoreCase(query, " FOR UPDATE")) && !StrContainWithIgnoreCase(query, " NOWAIT") {
		panic(PanicLockingReadMustUseNowait)
	}

	if tx == nil {
		tx = DB
	}

	rows, err := tx.Query(query, args...)
	if err != nil {
		if e := isAssumedSQLError(err); e != nil {
			return nil, e
		}
		panic(fmt.Sprintf("query failed: %s, failed query: %s", err, query))
	}

	// rowsの処理はクエリの実行後のエラーチェックが完了した後に呼ぶ。
	// （そのようにしなければpanicが発生する恐れがある）
	//
	// rowsで表現される結果セットがある限りコネクションはビジー状態であり、
	// このコネクションはコネクションプールにおいて他のクエリで利用できない。
	// したがって、必ず最後にrowsのcloseが実行されることを保証する必要がある。
	// rows.Next()では、終了時やエラー時には自動的にClose()が呼ばれる仕様になっているが、
	// 例えばループ内の各処理でエラー発生時に早期リターンなどをした際は
	// rowsはクローズされず、コネクションもオープンのまま、となる。
	// したがってどのケースでも常にClose()されるように、deferでCloseを呼び出しておく。
	// Closeは既にクローズされている場合には何もしないため、重複しても問題ない。
	// なお、deferはpanicの際も必ず実行される。
	defer rows.Close()

	// 以下の情報を利用してScanへ渡すstructの各フィールドへのポインタ配列を作成する。
	// ・モデルで定義したstructのフィールドの型とタグ情報
	// ・結果セット（rows）のフィールド名
	//
	// ※ この処理の目的: Scan関数へ渡すポインタ配列の順番を、DBからの取得結果（rows）の
	//   各フィールドの順番と合わせる必要があるため。
	//  （そのまま構造体の各フィールドを渡すと順番が不一致となってしまう）
	structValue := *mp
	structElem := reflect.ValueOf(&structValue).Elem()
	structType := structElem.Type()
	if structType.Kind() != reflect.Struct {
		panic("model mubt be struct.")
	}
	// 計算量をO(構造体のフィールド数+結果セットのカラム数)とするため、mapにしておく。
	structFieldNameToTypeMap := make(map[string]interface{})
	for i := 0; i < structType.NumField(); i++ {
		columnName := structType.Field(i).Tag.Get("database")
		// タグはすべてのフィールドに設定されている必要がある。
		if columnName == "" {
			n := structType.Field(i).Name
			panic(fmt.Sprintf("%s has no database label.", n))
		}
		// Scan等のinterface{}を受け取る関数は、内部で型情報を復元するため、
		// ここではすべてのフィールドはその型に関係なく最後にinterface{}にしておけば良い。
		structFieldNameToTypeMap[columnName] = structElem.Field(i).Addr().Interface()
	}
	ct, err := rows.ColumnTypes()
	if err != nil {
		panic(err)
	}
	structFieldValuePtrInterfaces := make([]interface{}, len(ct))
	for i, c := range ct {
		structFieldAddr, ok := structFieldNameToTypeMap[c.Name()]
		// 結果セットのフィールドが、モデルのタグに含まれていない場合はpanic
		if !ok {
			panic(fmt.Sprint("model does not have result field: ", c.Name()))
		}
		structFieldValuePtrInterfaces[i] = structFieldAddr
	}

	// rows.Next()は全ての行を繰り返し処理すると、
	// 最終的には最後の行が読み込まれ、rows.Next()内部でEOFエラーが発生し、
	// rows.Close()を呼び出す。
	// rows.Next()で何らかのエラーが発生した場合もrows.Close()が呼ばれる。
	r := []M{}
	for rows.Next() {
		structValue = *mp

		// ※ Scanは内部で型変換をしてくれる
		if err := rows.Scan(structFieldValuePtrInterfaces...); err != nil {
			panic(err)
		}
		r = append(r, structValue)
	}

	// rows.Err() からのエラーはループ内のさまざまなエラーの結果である可能性があるため、
	// ここで必ずチェックしておく必要がある。
	err = rows.Err()
	if err != nil {
		panic(err)
	}

	// デバッグモードの場合はExplainによるチェックを行う
	if isDebugMode() && !CheckSeqScan(query, args...) {
		panic(fmt.Sprintf(PanicSQLIsSeqScan, query))
	}

	return r, nil
}

// "Seq Scan"のSQLが存在する場合はただちにpanicで処理を止めて出力。
func CheckSeqScan(query string, args ...any) bool {
	if !useSeqScanCheck || StrContainWithIgnoreCase(query, seqScanCheckDisableClause) {
		return true
	}

	if !isDebugMode() {
		panic("not use this function without debug mode")
	}
	tx, err := DB.Begin()

	if err != nil {
		panic(err)
	}

	// データが少ない場合でも"Seq Scan"に最適化されないように`enable_seqscan`をoffにしておく。
	// LOCAL: トランザクション単位
	// デフォルトはSESSION単位だが同じコネクションを使っている他のSQLも全て
	// 影響してしまうため、LOCALとしている。
	_, err = tx.Exec("SET LOCAL enable_seqscan TO 'off'")
	if err != nil {
		panic(fmt.Sprintf("SET exec failed: %s", err))
	}

	// analyzeは実際にSQLが実行されてしまうためfalseとしている。
	rows, err := tx.Query("EXPLAIN (ANALYZE false, FORMAT json) "+query, args...)
	if err != nil {
		panic(fmt.Sprintf("query failed: %s, failed query: %s", err, query))
	}
	defer rows.Close()
	r := []string{}
	for rows.Next() {
		var s string
		if err := rows.Scan(&s); err != nil {
			panic(err)
		}
		r = append(r, s)
	}
	err = rows.Err()
	if err != nil {
		panic(err)
	}
	// Explainでは特にコミットするものはないためロールバックをしている。
	if err := tx.Rollback(); err != nil {
		panic(err)
	}

	if len(r) != 1 {
		panic("explain result is not 1 row")
	}
	p := []Plan{}
	err = json.Unmarshal([]byte(r[0]), &p)
	if err != nil {
		panic(err)
	}
	if len(p) != 1 {
		panic("explain result json is not 1 child")
	}

	// "Seq Scan"が含まれている場合はpanicとする。
	// 構造体にマッピングせずに文字列による検索でも実現はできるが、
	// 管理のしやすさのために構造体に格納している。
	//
	// [参考]
	// https://www.postgresql.jp/docs/14/using-explain.html
	// ・Node Typeの種類
	// "Seq Scan": 全検索
	// "Index Scan": インデックスを使った検索
	// "Bitmap Index Scan": インデックスを使って検索。（「Bitmap」はソートの機構の名前となる。）
	//  Bitmapをビルドするため、通常のインデックススキャンよりはコストが大きい。
	// "Bitmap Heap Scan":  「Bitmap Index Scan」の結果を取り出す上位の計画、と考えられる
	// "Result": insertとかupdateの結果
	// "ModifyTable": insert や　updateを使うと上位に現れる計画
	// "LockRows":  FOR UPDATEを使うと上位に現れる計画
	// "Limit": limit句を使うと上位に現れる計画
	// "Sort": order by句を使うと上位に現れる計画
	//
	// ・ネスト（複数段階の計画）
	// 例えば where user_id = $1 order by created_at limit 50 の場合、
	// Node Type: Limit >  Node Type: Sort >  Node Type: Index Scan といった具合の
	// ３段階の計画となる。これは最下層のIndex Scanから行われる。
	//
	// ・"Seq Scan"と"Index Scan"
	// テーブルの件数等によって、最適な実行計画がオプティマイザーによって選択される。
	// データ数が少ない場合だと"Index Scan"よりも"Seq Scan"の方が効率的として
	// そちらが選択される。（例えば xxx = 'a' OR xxx = 'b' 等の条件で確認できる）
	// したがって本チェックでは冒頭で「enable_seqscan」をoffにすることで、どちらも選択
	// 可能な際は"Seq Scan"を選択しないように設定している。
	plansHaveSeqScan := false
	for _, ps1 := range p[0].Plan.Plans {
		if StrContainWithIgnoreCase(ps1.NodeType, "Seq Scan") {
			plansHaveSeqScan = true
		}
		for _, ps2 := range ps1.Plans {
			if StrContainWithIgnoreCase(ps2.NodeType, "Seq Scan") {
				plansHaveSeqScan = true
			}
			for _, ps3 := range ps2.Plans {
				if StrContainWithIgnoreCase(ps3.NodeType, "Seq Scan") {
					plansHaveSeqScan = true
				}
				for _, ps4 := range ps3.Plans {
					if StrContainWithIgnoreCase(ps4.NodeType, "Seq Scan") {
						plansHaveSeqScan = true
					}
					for _, ps5 := range ps4.Plans {
						if StrContainWithIgnoreCase(ps5.NodeType, "Seq Scan") {
							plansHaveSeqScan = true
						}
						for _, ps6 := range ps5.Plans {
							if StrContainWithIgnoreCase(ps6.NodeType, "Seq Scan") {
								plansHaveSeqScan = true
							}
							for _, ps7 := range ps6.Plans {
								if StrContainWithIgnoreCase(ps7.NodeType, "Seq Scan") {
									plansHaveSeqScan = true
								}
								for _, ps8 := range ps7.Plans {
									if StrContainWithIgnoreCase(ps8.NodeType, "Seq Scan") {
										plansHaveSeqScan = true
									}
									for _, ps9 := range ps8.Plans {
										if StrContainWithIgnoreCase(ps9.NodeType, "Seq Scan") {
											plansHaveSeqScan = true
										}
										for _, ps10 := range ps9.Plans {
											if StrContainWithIgnoreCase(ps10.NodeType, "Seq Scan") {
												plansHaveSeqScan = true
											}
										}
									}
								}
							}
						}
					}
				}
			}
		}
	}
	if StrContainWithIgnoreCase(p[0].Plan.NodeType, "Seq Scan") || plansHaveSeqScan {
		m := []map[string]any{}
		err = json.Unmarshal([]byte(r[0]), &m)
		if err != nil {
			panic(err)
		}
		{
			return false
		}
	}
	return true
}

// 注: これ以上ネストされた結果の場合は情報をロストする。
type Plan struct {
	Plan struct {
		NodeType string `json:"Node Type"`
		Plans    []struct {
			NodeType string `json:"Node Type"`
			Plans    []struct {
				NodeType string `json:"Node Type"`
				Plans    []struct {
					NodeType string `json:"Node Type"`
					Plans    []struct {
						NodeType string `json:"Node Type"`
						Plans    []struct {
							NodeType string `json:"Node Type"`
							Plans    []struct {
								NodeType string `json:"Node Type"`
								Plans    []struct {
									NodeType string `json:"Node Type"`
									Plans    []struct {
										NodeType string `json:"Node Type"`
										Plans    []struct {
											NodeType string `json:"Node Type"`
											Plans    []struct {
												NodeType string `json:"Node Type"`
											} `json:"Plans"`
										} `json:"Plans"`
									} `json:"Plans"`
								} `json:"Plans"`
							} `json:"Plans"`
						} `json:"Plans"`
					} `json:"Plans"`
				} `json:"Plans"`
			} `json:"Plans"`
		} `json:"Plans"`
	} `json:"Plan"`
}

func Exec(tx HasExec, query string, args ...any) (sql.Result, error) {
	// プレースホルダー（$）とargsの個数が一致しない場合はエラーとする。
	if strings.Count(query, "$") != len(args) {
		panic(PanicPlaceHolderNumberNotMatch)
	}

	if useWhereCheck && StrContainWithIgnoreCase(query, "DELETE ") && !StrContainWithIgnoreCase(query, " WHERE ") && !StrContainWithIgnoreCase(query, disableWhereCheckClause) {
		panic(PanicDeleteSQLMustUseWhere)
	}

	if StrContainWithIgnoreCase(query, "UPDATE ") {
		if useWhereCheck && !StrContainWithIgnoreCase(query, " WHERE ") && !StrContainWithIgnoreCase(query, disableWhereCheckClause) {
			panic(PanicUpdateSQLMustUseWhere)
		}
		if forceUpdatedAtCheck && !StrContainWithIgnoreCase(query, "updated_at") {
			panic(PanicUpdateSQLMustHaveUpdatedAt)
		}
	}

	if tx == nil {
		tx = DB
	}

	result, err := tx.Exec(query, args...)
	if err != nil {
		if e := isAssumedSQLError(err); e != nil {
			return nil, e
		}
		panic(fmt.Sprintf("query failed: %s, failed query: %s", err, query))
	}

	// デバッグモードの場合はExplainによるチェックを行う
	if isDebugMode() && !CheckSeqScan(query, args...) {
		panic(fmt.Sprintf(PanicSQLIsSeqScan, query))
	}

	return result, nil
}

func isAssumedSQLError(err error) error {
	if strings.Contains(err.Error(), PostgresErrCodeLockNotAvailable) {
		return ErrLockNotAvailable
	}
	if strings.Contains(err.Error(), PostgresErrCodeUniqConstraint) {
		return ErrUniqConstraint
	}
	if strings.Contains(err.Error(), PostgresErrCodeDeadLock) {
		return ErrDeadLock
	}
	return nil
}

// トランザクションを生成して、受け取った無名関数へそのトランザクションを渡して実行する。
// エラーもpanicも発生せずに実行された場合は、トランザクションをコミットする。
// 無名関数の中でpanicが発生した場合はロールバックを実行する。
// 無名関数がerrorを返した場合はロールバックを実行した上でそのerrorを返す。
// この関数がerrorを返す場合は、それは無名関数が返したerrorとなる。
// (この関数自体の処理によって発生するエラーは無く、それらは全てpanicとなる)
//
// 今のところトランザクションのネストは想定していないので、txの引数は取っていない。
func Transaction(c context.Context, f func(*sql.Tx) error) error {
	tx, err := DB.Begin()
	if err != nil {
		panic(err)
	}
	if err := doAndRecover(c, tx, f); err != nil {
		// doAndRecover内で「f」の実行時にpanicが発生した場合は、
		// doAndRecover内でロールバックした上で、panicにしている。
		// その場合、（panicの仕様通り）以降の処理は実行されずpanicが呼び出し元へと伝搬していく。
		//
		// もしdoAndRecoverでこのrecover処理（ロールバック）を実行しない場合の問題として、
		// Go側の処理はpanicとして終了する一方、DB側ではトランザクションが仕掛り状態のまま残ってしまう。
		// つまりロックを取得している際は、そのロックが開放されず他のトランザクションへ影響が出てしまう。
		l.Info(c, "rollback start")
		// ロールバックに失敗するケースとして、考えられるのは、
		// ネットワークエラーやDB自体が停止している等。いずれにしても
		// 更新内容は消失する可能性が高い。（原子性が担保されていれば許容はできる）
		if err := tx.Rollback(); err != nil {
			panic(err)
		}
		l.Info(c, "rollback end")
		return err
	}

	// Commitが失敗しても成功してもコネクションはcloseされる。
	// なお、ロールバックもコミットもせずにcloseをすると、通常はロールバックされるはず。
	if err := tx.Commit(); err != nil {
		// トランザクションの中で既にエラーがあるにも関わらず
		// コミットをしている場合はpgxからErrTxCommitRollbackが返ってくる。
		// これはプログラムでちゃんとerrをチェックしていないということなので
		// panicにしている。
		if errors.Is(err, pgx.ErrTxCommitRollback) {
			panic(PanicCommitDespiteErrInTx)
		}
		// トランザクション中にエラーが発生せずにコミット時にエラーが出るケースは想定していない。
		panic(err)
	}
	return nil
}
