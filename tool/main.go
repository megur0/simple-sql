package main

import (
	"database/sql"
	"fmt"
	"os"
	"strconv"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/megur0/testutil"
)

var db *sql.DB

// テスト用
// DBの初期設定を行う

// env `cat .env` go run ./tool/main.go
func main() {
	openTestDB()
	defer db.Close()
	initDB()
	fmt.Println("db initialized done.")
}

func openTestDB() {
	if os.Getenv("TEST_DB_HOST") == "" || os.Getenv("DB_USER") == "" || os.Getenv("DB_PASSWORD") == "" || os.Getenv("DB_PORT_EXPOSE") == "" {
		panic("test db env is not set")
	}

	openDB(os.Getenv("TEST_DB_HOST"), os.Getenv("DB_USER"), os.Getenv("DB_PASSWORD"), testutil.GetFirst(strconv.Atoi(os.Getenv("DB_PORT_EXPOSE"))))
}

func openDB(dbHost, dbUser, dbPassword string, dbPort int) {
	var err error
	db, err = sql.Open("pgx", fmt.Sprintf(
		"user=%s password=%s host=%s port=%d dbname=%s sslmode=disable",
		dbUser, dbPassword, dbHost, dbPort, "test_db",
	))
	if err != nil {
		panic(fmt.Sprint("open db error: ", err))
	}
}

func initDB() {
	_, err := db.Exec(`ALTER DATABASE test_db SET timezone TO 'Asia/Tokyo'`)
	if err != nil {
		panic(err)
	}

	_, err = db.Exec(`CREATE EXTENSION IF NOT EXISTS "uuid-ossp"`)
	if err != nil {
		panic(err)
	}

	_, err = db.Exec(`
	CREATE TABLE IF NOT EXISTS "table_for_test" (
		"id" uuid NOT NULL DEFAULT uuid_generate_v4(),
		"uid" VARCHAR(500) NOT NULL,
		"name" text,
		"is_active" bool NOT NULL DEFAULT true,
		"created_at" timestamptz NOT NULL DEFAULT now(),
		"updated_at" timestamptz NOT NULL DEFAULT now(),
		PRIMARY KEY ("id")
	)`)
	if err != nil {
		panic(err)
	}

	_, err = db.Exec(`ALTER TABLE "table_for_test" DROP CONSTRAINT IF EXISTS "uniq__table_for_test__uid"`)
	if err != nil {
		panic(err)
	}

	_, err = db.Exec(`ALTER TABLE "table_for_test" ADD CONSTRAINT "uniq__table_for_test__uid" UNIQUE("uid")`)
	if err != nil {
		panic(err)
	}
}
