package ssql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/megur0/testutil"

	"github.com/google/uuid"

	_ "github.com/jackc/pgx/v5/stdlib"
)

// テストの実行の際は事前に以下の実行が必要
// ・テストDB(docker)の立ち上げ
//  docker compose up -d
// ・DBの初期設定
//  env `cat .env` go run ./tool/main.go

type TableForTest struct {
	ID        uuid.UUID `database:"id"`
	UID       string    `database:"uid"`
	Name      *string   `database:"name"`
	IsActive  bool      `database:"is_active"`
	CreatedAt time.Time `database:"created_at"`
	UpdatedAt time.Time `database:"updated_at"`
}

func openTestDB() {
	if os.Getenv("TEST_DB_HOST") == "" || os.Getenv("DB_USER") == "" || os.Getenv("DB_PASSWORD") == "" || os.Getenv("DB_PORT_EXPOSE") == "" {
		panic("test db env is not set")
	}

	openDB(os.Getenv("TEST_DB_HOST"), os.Getenv("DB_USER"), os.Getenv("DB_PASSWORD"), testutil.GetFirst(strconv.Atoi(os.Getenv("DB_PORT_EXPOSE"))), MODE_DEBUG)
}

// env `cat .env` go test -v -count=1 -timeout 60s ./sql
func TestMain(m *testing.M) {
	openTestDB()
	defer DB.Close()

	m.Run()
}

// env `cat .env` go test -v -count=1 -timeout 60s -run ^TestDB$ ./sql
func TestDB(t *testing.T) {
	t.Run("success_db_open_stats_close", func(t *testing.T) {
		closeDB()
		openTestDB()
		stats()
	})
}

// env `cat .env` go test -v -count=1 -timeout 60s -run ^TestSQL$ ./sql
func TestSQL(t *testing.T) {
	refreshDB()

	t.Run("success_select_where", func(t *testing.T) {
		_, err := Query(nil, &TableForTest{}, "SELECT * FROM table_for_test WHERE id=$1", "69e00805-dbc9-4a12-b43f-65b0fd6c5023")
		if err != nil {
			t.Error("got error")
		}
	})

	t.Run("success_select_where_any", func(t *testing.T) {
		_, err := Query(nil, &TableForTest{}, "SELECT * FROM table_for_test WHERE id = Any($1)", []string{"69e00805-dbc9-4a12-b43f-65b0fd6c5023", "69e00805-dbc9-4a12-b43f-65b0fd6c5023"})
		if err != nil {
			t.Error("got error")
		}
	})

	t.Run("success_insert", func(t *testing.T) {
		result, err := Exec(nil, "INSERT INTO table_for_test (name, uid) VALUES ($1, $2)", "aaaaaa", "a")
		if err != nil {
			t.Fatal("got error")
		}
		id, _ := result.LastInsertId()
		row, _ := result.RowsAffected()
		df("%v, %v", id, row)
		testutil.AssertEqual(t, int(row), 1)
	})

	t.Run("success_update", func(t *testing.T) {
		result, err := Exec(nil, "UPDATE table_for_test SET name=$1, updated_at=$2 WHERE uid=$3", "bbbbbb", time.Now(), "a")
		if err != nil {
			t.Fatal("got error")
		}
		id, _ := result.LastInsertId()
		row, _ := result.RowsAffected()
		df("%v, %v", id, row)
		testutil.AssertEqual(t, int(row), 1)
	})

	t.Run("success_select_with_nil_result", func(t *testing.T) {
		r, err := QueryFirst(nil, &TableForTest{}, "SELECT * FROM table_for_test WHERE uid='5'")
		if err != nil {
			t.Fatal("got error")
		}
		if r != nil {
			t.Error("result should be nil")
		}
	})
}

// ユニーク制約エラーのハンドリング
// env `cat .env` go test -v -count=1 -timeout 60s -run ^TestUniqError$ ./sql
func TestUniqError(t *testing.T) {
	refreshDB()

	r, err := Exec(nil, "INSERT INTO table_for_test (name, uid) VALUES ($1, $2)", "aaaaaa", "a")
	if err != nil {
		t.Fatal("got error")
	}
	row, _ := r.RowsAffected()
	testutil.AssertEqual(t, int(row), 1)

	t.Run("uniq_error", func(t *testing.T) {
		_, err := Exec(nil, "INSERT INTO table_for_test (name, uid) VALUES ($1, $2)", "aaaaaa", "a")
		if err == nil {
			t.Fatal("should got error")
		}
		testutil.AssertEqual(t, err, ErrUniqConstraint)
		dv(err)
	})
}

// トランザクションブロックにおけるユニーク制約エラー
// env `cat .env` go test -v -count=1 -timeout 60s -run ^TestUniqErrorAtCommit$ ./sql
func TestUniqErrorAtCommit(t *testing.T) {
	refreshDB()

	uid := "aaaa"

	t.Run("insert_fail_because_uniq_constraint", func(t *testing.T) {
		syn1 := make(chan interface{}, 1)
		syn2 := make(chan interface{}, 1)
		var wg sync.WaitGroup

		wg.Add(1)
		var err error
		go func() {
			err = Transaction(context.Background(), func(tx *sql.Tx) error {
				re, err := Exec(tx, "INSERT INTO table_for_test (name, uid) VALUES ($1, $2)", "aaaaaa", uid)
				row, _ := re.RowsAffected()
				testutil.AssertEqual(t, err, nil)
				testutil.AssertEqual(t, int(row), 1)

				syn1 <- struct{}{} // insertまで終わったら2つ目のスレッドへ知らせる。
				d("g1 syn1 sent and syn2 wait...")

				<-syn2 // ２つ目のスレッドのqueryが終わるのを待つ。
				d("g1 syn2 recieved")

				return nil
			})
			testutil.AssertEqual(t, err, nil)
			d("g1 commit done")
			wg.Done()
		}()

		wg.Add(1)
		go func() {
			err = Transaction(context.Background(), func(tx *sql.Tx) error {
				var u []TableForTest

				<-syn1 //１つ目のスレッドがinsertをするまで待つ。
				d("g2 syn1 recieved")

				u, err = Query(tx, &TableForTest{}, "SELECT * FROM table_for_test WHERE uid=$1", uid)
				testutil.AssertEqual(t, err, nil)
				testutil.AssertEqual(t, len(u), 0) // リードコミッティドなので何も取得されない。

				syn2 <- struct{}{}
				d("g2 query done and sent syn2.")

				// もし、まだ１つ目のスレッドがコミットしていない場合でも、
				// uniq制約違反の恐れがあるとしてこちらのinsertは待機状態にされる。
				// そして、１つ目のスレッドがコミットを完了した後にこちら側がエラーになる。
				// uniq制約はMVCCで動作するため、分離レベルに関係ない。
				// （リードコミッティド以上でも、コミットされていないデータに対して制約が適用される。）
				_, err := Exec(tx, "INSERT INTO table_for_test (name, uid) VALUES ($1, $2)", "aaaaaa", uid)
				d("g2 insert done")
				dv(err)
				testutil.AssertEqual(t, err, ErrUniqConstraint) // uniq制約違反になる。

				if err != nil {
					return err
				}

				return nil
			})
			testutil.AssertEqual(t, err, ErrUniqConstraint) // uniq制約違反になる。
			d("g2 commit done")
			wg.Done()
		}()

		wg.Wait()
	})
}

// panicが発生した時にロックが開放されることの確認。
// env `cat .env` go test -v -count=1 -timeout 60s -run ^TestPanicLock$ ./sql
func TestPanicLock(t *testing.T) {
	refreshDB()

	uid := "aa"
	Exec(nil, "INSERT INTO table_for_test (name, uid) VALUES ($1, $2)", "aaaa", uid)

	t.Run("lock_is_released_when_panic_occured", func(t *testing.T) {
		syn1 := make(chan interface{}, 1)
		syn2 := make(chan interface{}, 1)
		var wg sync.WaitGroup

		wg.Add(1)
		var err error
		go func() {
			defer func() {
				recover()

				syn1 <- struct{}{} // 取得したら2つ目のスレッドへ知らせる。
				d("g1 syn1 sent and syn2 wait...")

				<-syn2 // ２つ目のスレッドのqueryが終わるのを待つ。
				d("g1 syn2 recieved")

				wg.Done()
			}()
			err = Transaction(context.Background(), func(tx *sql.Tx) error {
				var p []TableForTest
				p, err = Query(tx, &TableForTest{}, "SELECT * FROM table_for_test WHERE uid=$1 FOR UPDATE NOWAIT", uid)
				testutil.AssertEqual(t, err, nil)
				testutil.AssertEqual(t, len(p), 1)

				panic("") // panicを起こす。
			})
		}()

		wg.Add(1)
		go func() {
			err = Transaction(context.Background(), func(tx *sql.Tx) error {
				<-syn1 //１つ目のスレッドがロックをかけるまで待つ。
				d("g2 syn1 recieved")

				_, err = Query(tx, &TableForTest{}, "SELECT * FROM table_for_test WHERE uid=$1 FOR UPDATE NOWAIT", uid)
				testutil.AssertEqual(t, err, nil) // ErrLockNotAvailableにならないこと

				syn2 <- struct{}{}
				d("g2 query done and sent syn2.")
				return err
			})
			testutil.AssertEqual(t, err, nil) // ErrLockNotAvailableにならないこと
			d("g2 commit done")
			wg.Done()
		}()

		wg.Wait()
	})
}

// ユニーク制約によるデッドロック。
// env `cat .env` go test -v -count=1 -timeout 60s -run ^TestDeadLock$ ./sql
func TestDeadLock(t *testing.T) {
	refreshDB()

	uid1 := "aa"
	uid2 := "bb"

	t.Run("fail", func(t *testing.T) {
		syn1 := make(chan interface{}, 1)
		syn2 := make(chan interface{}, 1)
		var wg sync.WaitGroup

		wg.Add(1)
		var err1 error
		go func() {
			err1 = Transaction(context.Background(), func(tx *sql.Tx) error {
				Exec(tx, "INSERT INTO table_for_test (name, uid) VALUES ($1, $2)", "aaaa", uid1)

				syn1 <- struct{}{} // insertが終わったら2つ目のスレッドへ知らせる。
				d("g1 syn1 sent and syn2 wait...")
				<-syn2 // ２つ目のスレッドがinsertするのを待つ。

				_, err := Exec(tx, "INSERT INTO table_for_test (name, uid) VALUES ($1, $2)", "bbbb", uid2) // ここでロック待ちになる。

				// デッドロックが検出された場合は、片方は成功し、(errはnil)
				// 片方はdead lockとして失敗する。
				df("g1 error: %v", err)
				if err != nil {
					return err
				}

				return nil
			})
			if err1 == nil {
				d("g1 commit success")
			}
			wg.Done()
		}()

		wg.Add(1)
		var err2 error
		go func() {
			err2 = Transaction(context.Background(), func(tx *sql.Tx) error {

				<-syn1 //１つ目のスレッドがinsertをするまで待つ。
				d("g2 syn1 recieved")

				Exec(tx, "INSERT INTO table_for_test (name, uid) VALUES ($1, $2)", "bbbb", uid2)

				syn2 <- struct{}{} // insertしたら スレッド1に知らせる。
				d("g2 syn2 sent")

				_, err := Exec(tx, "INSERT INTO table_for_test (name, uid) VALUES ($1, $2)", "aaaa", uid1) // ここでロック待ちになる。

				df("g2 error: %v", err)
				if err != nil {
					return err
				}

				return nil
			})
			if err2 == nil {
				d("g2 commit success")
			}
			wg.Done()
		}()

		wg.Wait()
		if !errors.Is(err1, ErrDeadLock) && !errors.Is(err2, ErrDeadLock) {
			df("%v, %v", err1, err2)
			t.Error("should be deadlock")
		}
	})

}

// トランザクションブロックにおける行ロック待ちの検証
// env `cat .env` go test -v -count=1 -timeout 60s -run ^TestLockWaitAtSameRow$ ./sql
func TestLockWaitAtSameRow(t *testing.T) {
	refreshDB()
	uid := "aa"
	t.Run("row_lock_wait_at_update", func(t *testing.T) {
		Exec(nil, "INSERT INTO table_for_test (uid, name) VALUES ($1, $2)", uid, "aaaa")
		m, _ := QueryFirst(nil, &TableForTest{}, "SELECT * FROM table_for_test WHERE uid=$1", uid)

		syn1 := make(chan interface{}, 1)
		var wg sync.WaitGroup

		wg.Add(1)
		var err error
		check := 0
		go func() {
			err = Transaction(context.Background(), func(tx *sql.Tx) error {
				_, err = Exec(tx, "UPDATE table_for_test SET updated_at=$1 WHERE uid=$2", time.Now(), m.UID)
				testutil.AssertEqual(t, err, nil)

				syn1 <- struct{}{} // updateが終わったら2つ目のスレッドへ知らせる。
				d("g1 syn1 sent")
				d("g1 sleep...") // ここでsleepが入っても必ずg1が先に終わる。
				time.Sleep(time.Millisecond * 100)

				check = 1
				return nil
			})
			testutil.AssertEqual(t, err, nil)
			d("g1 commit done")
			wg.Done()
		}()

		wg.Add(1)
		go func() {
			err = Transaction(context.Background(), func(tx *sql.Tx) error {
				<-syn1 //１つ目のスレッドがupdateをするまで待つ。
				d("g2 syn1 recieved")

				// まだ１つ目のスレッドがコミットしていない場合でも、
				// 同一のレコードの更新のため待機が発生する。
				// （リードコミッティドでも、コミットされていないデータに対して制約が適用される。）
				_, err = Exec(tx, "UPDATE table_for_test SET updated_at=$1 WHERE uid=$2", time.Now(), m.UID)
				testutil.AssertEqual(t, err, nil)
				if err != nil {
					return err
				}

				check = 2
				return nil
			})
			testutil.AssertEqual(t, err, nil)
			d("g2 commit done")
			wg.Done()
		}()

		wg.Wait()
		testutil.AssertEqual(t, check, 2) // かならずスレッド2が後に終わる。
	})

	refreshDB()
	uid2 := "bb"
	t.Run("no_wait_at_update_because_not_same_row", func(t *testing.T) {
		Exec(nil, "INSERT INTO table_for_test (uid, name) VALUES ($1, $2)", uid, "aaaa")
		Exec(nil, "INSERT INTO table_for_test (uid, name) VALUES ($1, $2)", uid2, "bbbb")
		m, _ := QueryFirst(nil, &TableForTest{}, "SELECT * FROM table_for_test WHERE uid=$1", uid)
		m2, _ := QueryFirst(nil, &TableForTest{}, "SELECT * FROM table_for_test WHERE uid=$1", uid2)

		syn1 := make(chan interface{}, 1)
		var wg sync.WaitGroup

		wg.Add(1)
		var err error
		check := 0
		go func() {
			err = Transaction(context.Background(), func(tx *sql.Tx) error {
				_, err = Exec(tx, "UPDATE table_for_test SET updated_at=$1 WHERE uid=$2", time.Now(), m.UID)
				testutil.AssertEqual(t, err, nil)

				syn1 <- struct{}{} // updateまで終わったら2つ目のスレッドへ知らせる。
				d("g1 syn1 sent")
				d("g1 sleep...") // ここでsleepが入れているのでg2が先に終わる。
				time.Sleep(time.Millisecond * 100)

				check = 1
				return nil
			})
			testutil.AssertEqual(t, err, nil)
			d("g1 commit done")
			wg.Done()
		}()

		wg.Add(1)
		go func() {
			err = Transaction(context.Background(), func(tx *sql.Tx) error {
				<-syn1 //１つ目のスレッドがupdateをするまで待つ。
				d("g2 syn1 recieved")

				// 対象のレコードが異なるので、waitは発生しない。
				_, err = Exec(tx, "UPDATE table_for_test SET updated_at=$1 WHERE uid=$2", time.Now(), m2.UID)
				testutil.AssertEqual(t, err, nil)
				if err != nil {
					return err
				}

				check = 2
				return nil
			})
			testutil.AssertEqual(t, err, nil)
			d("g2 commit done")
			wg.Done()
		}()

		wg.Wait()
		testutil.AssertEqual(t, check, 1) // 2が先に終わることを確認。
	})

	refreshDB()
	t.Run("row_lock_wait_at_update", func(t *testing.T) {
		Exec(nil, "INSERT INTO table_for_test (uid, name) VALUES ($1, $2)", uid, "aaaa")
		m, _ := QueryFirst(nil, &TableForTest{}, "SELECT * FROM table_for_test WHERE uid=$1", uid)

		syn1 := make(chan interface{}, 1)
		var wg sync.WaitGroup

		wg.Add(1)
		var err error
		check := 0
		go func() {
			err = Transaction(context.Background(), func(tx *sql.Tx) error {
				_, err = Exec(tx, "UPDATE table_for_test SET updated_at=$1 WHERE uid=$2", time.Now(), m.UID)
				testutil.AssertEqual(t, err, nil)

				syn1 <- struct{}{} // updateが終わったら2つ目のスレッドへ知らせる。
				d("g1 syn1 sent")
				d("g1 sleep...")
				time.Sleep(time.Millisecond * 100)

				check = 1
				return nil
			})
			testutil.AssertEqual(t, err, nil)
			d("g1 commit done")
			wg.Done()
		}()

		wg.Add(1)
		go func() {
			err = Transaction(context.Background(), func(tx *sql.Tx) error {
				<-syn1 //１つ目のスレッドがupdateをするまで待つ。
				d("g2 syn1 recieved")

				// select は ACCESS SHARE を獲得し、
				// これは EXCLUSIVEおよびACCESS EXCLUSIVE のみと競合するため、
				// この場合は競合しない。したがってwaitは発生しない。
				// https://www.postgresql.jp/document/14/html/explicit-locking.html
				QueryFirst(nil, &TableForTest{}, "SELECT * FROM table_for_test WHERE uid=$1", m.UID)
				testutil.AssertEqual(t, err, nil)
				if err != nil {
					return err
				}

				check = 2
				return nil
			})
			testutil.AssertEqual(t, err, nil)
			d("g2 commit done")
			wg.Done()
		}()

		wg.Wait()
		testutil.AssertEqual(t, check, 1) // スレッド1が後に終わる。
	})

}

// ロッキングリードでのエラーハンドリングの確認
// env `cat .env` go test -v -count=1 -timeout 60s -run ^TestLockError$ ./sql
func TestLockError(t *testing.T) {
	refreshDB()

	_, err := Exec(nil, "INSERT INTO table_for_test (uid) VALUES ($1)", "a")
	if err != nil {
		t.Fatalf("got error")
	}
	t.Run("fail", func(t *testing.T) {
		syn1 := make(chan interface{}, 1)
		syn2 := make(chan interface{}, 1)
		var wg sync.WaitGroup

		wg.Add(1)
		var err error
		go func() {
			err = Transaction(context.Background(), func(tx *sql.Tx) error {
				_, err := Query(tx, &TableForTest{}, "SELECT * FROM table_for_test WHERE uid=$1 FOR UPDATE NOWAIT", "a")
				if err != nil {
					return err
				}
				syn1 <- struct{}{}
				<-syn2
				return nil
			})
			wg.Done()
		}()
		testutil.AssertEqual(t, err, nil)

		err = Transaction(context.Background(), func(tx *sql.Tx) error {
			<-syn1
			_, err := QueryFirst(tx, &TableForTest{}, "SELECT * FROM table_for_test WHERE uid=$1 FOR UPDATE NOWAIT", "a")
			if err != nil {
				syn2 <- struct{}{}
				return err
			}
			return nil
		})
		testutil.AssertEqual(t, err, ErrLockNotAvailable)
		wg.Wait()
	})

}

// プレースホルダーの不正、SQLインジェクション、Whereを含まないdelete、他
// env `cat .env` go test -v -count=1 -timeout 60s -run ^TestInvalidSQL$ ./sql
func TestInvalidSQL(t *testing.T) {
	refreshDB()

	t.Run("fail_place_holder_invalid", func(t *testing.T) {
		var r interface{}
		defer func() {
			if r = recover(); r == nil {
				t.Fatalf("should get panic")
			}
			testutil.AssertEqual(t, r, PanicPlaceHolderNumberNotMatch)
		}()
		_, err := Query(nil, &TableForTest{}, "SELECT * FROM table_for_test WHERE id=$1 AND id=$2", "a")
		if err != nil {
			t.Fatalf("should not get error")
		}
	})

	t.Run("fail_not_use_nowait", func(t *testing.T) {
		var r interface{}
		defer func() {
			if r = recover(); r == nil {
				t.Fatalf("should get panic")
			}
			testutil.AssertEqual(t, r, PanicLockingReadMustUseNowait)
		}()
		_, err := Query(nil, &TableForTest{}, "SELECT * FROM table_for_test WHERE uid=a FOR SELECT")
		if err != nil {
			t.Fatalf("should not get error")
		}
	})

	t.Run("fail_at_sql_injection", func(t *testing.T) {
		var r interface{}
		defer func() {
			if r = recover(); r == nil {
				t.Fatalf("should get panic")
			}
			testutil.AssertContainStr(t, r, PostgresErrCodeInvalidSyntax)
		}()
		_, err := Query(nil, &TableForTest{}, "SELECT * FROM table_for_test WHERE id=$1", "id;SELECT * FROM table_for_test")
		if err != nil {
			t.Fatalf("should not get error")
		}
	})

	Exec(nil, "INSERT INTO table_for_test (uid) VALUES ($1)", "a")

	t.Run("fail_not_select_at_query", func(t *testing.T) {
		var r interface{}
		defer func() {
			if r = recover(); r == nil {
				t.Fatalf("should get panic")
			}
			testutil.AssertEqual(t, r, PanicQueryNotContanSelect)
		}()
		_, err := Query(nil, &TableForTest{}, "DELETE FROM table_for_test")
		if err != nil {
			t.Fatalf("should not get error")
		}
	})

	t.Run("fail_delete_have_no_where", func(t *testing.T) {
		var r interface{}
		defer func() {
			if r = recover(); r == nil {
				t.Fatalf("should get panic")
			}
			testutil.AssertEqual(t, r, PanicDeleteSQLMustUseWhere)
		}()
		_, err := Exec(nil, "DELETE FROM table_for_test")
		if err != nil {
			t.Fatalf("should not get error")
		}
	})

	t.Run("user_is_not_deleted", func(t *testing.T) {
		u, _ := QueryFirst(nil, &TableForTest{}, "SELECT * FROM table_for_test WHERE uid=$1", "a")
		if u == nil {
			t.Fatalf("user not exists")
		}
	})

	t.Run("fail_because_not_have_where", func(t *testing.T) {
		var r interface{}
		defer func() {
			if r = recover(); r == nil {
				t.Fatalf("should get panic")
			}
			testutil.AssertEqual(t, r, PanicSelectSQLMustUseWhere)
		}()
		_, err := Query(nil, &TableForTest{}, "SELECT * FROM table_for_test")
		if err != nil {
			t.Fatalf("should not get error")
		}
	})

}

// env `cat .env` go test -v -count=1 -timeout 60s -run ^TestColumnConstraint$ ./sql
func TestColumnConstraint(t *testing.T) {
	refreshDB()
	t.Run("ok_length_varchar", func(t *testing.T) {
		Exec(nil, "INSERT INTO table_for_test (name, uid) VALUES ($1, $2)", "aa", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa") //500文字

		Exec(nil, "INSERT INTO table_for_test (name, uid) VALUES ($1, $2)", "aa", "ああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああ") //500文字

		// 上限を超えた部分にある文字がすべて空白の場合はエラーにはならず、
		// 文字列の最大長にまで切り詰められる。
		// https://www.postgresql.jp/docs/14/datatype-character.html
		Exec(nil, "INSERT INTO table_for_test (name, uid) VALUES ($1, $2)", "aa", "ああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああああ         ") //500文字+半角空白
	})

	t.Run("over_length_varchar", func(t *testing.T) {
		var r interface{}
		defer func() {
			if r = recover(); r == nil {
				t.Fatalf("should get panic")
			}
			testutil.AssertContainStr(t, r, "SQLSTATE 22001")
		}()

		Exec(nil, "INSERT INTO table_for_test (name, uid) VALUES ($1, $2)", "aa", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa") //501文字
	})
}

// env `cat .env` go test -v -count=1 -timeout 60s -run ^TestIsNotSeqScanSQL$ ./sql
func TestIsNotSeqScanSQL(t *testing.T) {
	t.Run("panic_IsNotSeqScanSQL", func(t *testing.T) {
		var r interface{}
		defer func() {
			if r = recover(); r == nil {
				t.Fatalf("should get panic")
			}
			testutil.AssertEqual(t, r, fmt.Sprintf(PanicSQLIsSeqScan, "SELECT * FROM table_for_test WHERE name = $1"))
		}()
		_, err := Query(nil, &TableForTest{}, "SELECT * FROM table_for_test WHERE name = $1", "aaaaa")
		if err != nil {
			t.Fatalf("should not get error")
		}
	})

	t.Run("IsNotSeqScanSQ", func(t *testing.T) {
		testutil.AssertTrue(t, CheckSeqScan("SELECT name FROM table_for_test WHERE '"+SeqScanCheckDisableClause+"' = '"+SeqScanCheckDisableClause+"'"))
		testutil.AssertFalse(t, CheckSeqScan("SELECT name FROM table_for_test WHERE name = $1", "aaaaa"))
		testutil.AssertFalse(t, CheckSeqScan("SELECT name FROM table_for_test WHERE name = ANY(SELECT name FROM table_for_test WHERE name = $1)", "aaaaa"))
		testutil.AssertFalse(t, CheckSeqScan("SELECT name FROM table_for_test WHERE name = ANY(SELECT name FROM table_for_test WHERE name = ANY(SELECT name FROM table_for_test WHERE name = $1))", "aaaaa"))
		testutil.AssertFalse(t, CheckSeqScan("SELECT name FROM table_for_test WHERE name = ANY(SELECT name FROM table_for_test WHERE name = ANY(SELECT name FROM table_for_test WHERE name = ANY(SELECT name FROM table_for_test WHERE name = $1)))", "aaaaa"))
		testutil.AssertFalse(t, CheckSeqScan("SELECT name FROM table_for_test WHERE name = ANY(SELECT name FROM table_for_test WHERE name = ANY(SELECT name FROM table_for_test WHERE name = ANY(SELECT name FROM table_for_test WHERE name = ANY(SELECT name FROM table_for_test WHERE name = $1))))", "aaaaa"))
		testutil.AssertFalse(t, CheckSeqScan("SELECT name FROM table_for_test WHERE name = ANY(SELECT name FROM table_for_test WHERE name = ANY(SELECT name FROM table_for_test WHERE name = ANY(SELECT name FROM table_for_test WHERE name = ANY(SELECT name FROM table_for_test WHERE name = ANY(SELECT name FROM table_for_test WHERE name = $1)))))", "aaaaa"))
		testutil.AssertFalse(t, CheckSeqScan("SELECT name FROM table_for_test WHERE name = ANY(SELECT name FROM table_for_test WHERE name = ANY(SELECT name FROM table_for_test WHERE name = ANY(SELECT name FROM table_for_test WHERE name = ANY(SELECT name FROM table_for_test WHERE name = ANY(SELECT name FROM table_for_test WHERE name = ANY(SELECT name FROM table_for_test WHERE name = $1))))))", "aaaaa"))
		testutil.AssertFalse(t, CheckSeqScan("SELECT name FROM table_for_test WHERE name = ANY(SELECT name FROM table_for_test WHERE name = ANY(SELECT name FROM table_for_test WHERE name = ANY(SELECT name FROM table_for_test WHERE name = ANY(SELECT name FROM table_for_test WHERE name = ANY(SELECT name FROM table_for_test WHERE name = ANY(SELECT name FROM table_for_test WHERE name = ANY(SELECT name FROM table_for_test WHERE name = $1)))))))", "aaaaa"))
		testutil.AssertFalse(t, CheckSeqScan("SELECT name FROM table_for_test WHERE name = ANY(SELECT name FROM table_for_test WHERE name = ANY(SELECT name FROM table_for_test WHERE name = ANY(SELECT name FROM table_for_test WHERE name = ANY(SELECT name FROM table_for_test WHERE name = ANY(SELECT name FROM table_for_test WHERE name = ANY(SELECT name FROM table_for_test WHERE name = ANY(SELECT name FROM table_for_test WHERE name = ANY(SELECT name FROM table_for_test WHERE name = $1))))))))", "aaaaa"))
		testutil.AssertFalse(t, CheckSeqScan("SELECT name FROM table_for_test WHERE name = ANY(SELECT name FROM table_for_test WHERE name = ANY(SELECT name FROM table_for_test WHERE name = ANY(SELECT name FROM table_for_test WHERE name = ANY(SELECT name FROM table_for_test WHERE name = ANY(SELECT name FROM table_for_test WHERE name = ANY(SELECT name FROM table_for_test WHERE name = ANY(SELECT name FROM table_for_test WHERE name = ANY(SELECT name FROM table_for_test WHERE name = ANY(SELECT name FROM table_for_test WHERE name = $1)))))))))", "aaaaa"))
		testutil.AssertFalse(t, CheckSeqScan("SELECT name FROM table_for_test WHERE name = ANY(SELECT name FROM table_for_test WHERE name = ANY(SELECT name FROM table_for_test WHERE name = ANY(SELECT name FROM table_for_test WHERE name = ANY(SELECT name FROM table_for_test WHERE name = ANY(SELECT name FROM table_for_test WHERE name = ANY(SELECT name FROM table_for_test WHERE name = ANY(SELECT name FROM table_for_test WHERE name = ANY(SELECT name FROM table_for_test WHERE name = ANY(SELECT name FROM table_for_test WHERE name = ANY(SELECT name FROM table_for_test WHERE name = $1))))))))))", "aaaaa"))
	})
}

// env `cat .env` go test -v -count=1 -timeout 60s -run ^TestContainStr$ ./sql
func TestContainStr(t *testing.T) {
	for _, d := range []struct {
		target string
		str    string
		result bool
	}{
		{
			target: "DELETE FROM table_for_test WHERE id = $1",
			str:    "delete",
			result: true,
		},
		{
			target: "delete FROM table_for_test WHERE id = $1",
			str:    "DELETE",
			result: true,
		},
		{
			target: "delete FROM table_for_test WHERE id = $1",
			str:    "where",
			result: true,
		},
		{
			target: "deLete FROM table_for_test WHERE id = $1",
			str:    "delete",
			result: true,
		},
		{
			target: "deLete FROM table_for_test WHERE id = $1",
			str:    "delet",
			result: true,
		},
		{
			target: "deLete * FROM table_for_test WHERE id = $1",
			str:    "delett",
			result: false,
		},
		{
			target: "deLete * FROM table_for_test WHERE id = $1",
			str:    "where",
			result: true,
		},
		{
			target: "select * FROM table_for_test WHERE id = $1 FOR UPDATE",
			str:    "FOR UPDATE",
			result: true,
		},
		{
			target: "select * FROM table_for_test WHERE id = $1 FOR UPDATE",
			str:    "for update",
			result: true,
		},
		{
			target: "select * FROM table_for_test WHERE id = $1 FOR UPDATE",
			str:    "for select",
			result: false,
		},
	} {
		t.Run("assert", func(t *testing.T) {
			if StrContainWithIgnoreCase(d.target, d.str) != d.result {
				t.Error("unexpected result")
			}
		})
	}

	t.Run("success_StrContainList", func(t *testing.T) {
		testutil.AssertEqual(t, StrContainListWithIgnoreCase("watashi ha sample no text desu. こんにちは。", "sample", "ttt"), true)
		testutil.AssertEqual(t, StrContainListWithIgnoreCase("watashi ha sample no text desu. こんにちは。", "watasu", "ttt"), false)
	})
}

// env `cat .env` go test -v -count=1 -timeout 60s -run ^TestLog$ ./sql
func TestLog(t *testing.T) {
	l.Debug(context.Background(), "test %s", "test")
	l.Info(context.Background(), "test %s", "test")
	l.Warn(context.Background(), "test %s", "test")
	l.Error(context.Background(), "test %s", "test")
	d("test")
	dv("test")
	df("test %s %s", "arg1", "arg2")
	SetLogger(l)
}

func refreshDB() {
	dbRefresh([]string{"table_for_test"})
}

func openDB(dbHost, dbUser, dbPassword string, dbPort int, mode string) {
	Mode = mode

	var err error
	DB, err = sql.Open("pgx", fmt.Sprintf(
		"user=%s password=%s host=%s port=%d dbname=%s sslmode=disable",
		dbUser, dbPassword, dbHost, dbPort, "test_db",
	))
	if err != nil {
		panic(fmt.Sprint("open db error: ", err))
	}
}

// テスト用のDBリフレッシュ
func dbRefresh(tables []string) {
	if !IsDebugMode() {
		panic("db refresh only use at test")
	}

	// SEQUENCEは利用していないが、一応リセットしている(RESTART IDENTITY)
	_, err := DB.Exec("TRUNCATE " + strings.Join(tables, ",") + " RESTART IDENTITY")

	if err != nil {
		panic(err)
	}
}

func closeDB() {
	DB.Close()
}

func stats() {
	if IsDebugMode() {
		l.Debug(context.Background(), fmt.Sprintf("%+v", DB.Stats()))
	}
}

func d(message string) {
	l.Debug(context.Background(), message)
}

func df(message string, arg ...any) {
	l.Debug(context.Background(), fmt.Sprintf(message, arg...))
}

func dv(message any) {
	l.Debug(context.Background(), fmt.Sprintf("%v", message))
}
