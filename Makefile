.PHONY: test_all
test_all:
	env `cat .env` go test -v -count=1 -timeout 60s ./ssql

# ./tool/main.goはテスト用のデータ生成
.PHONY: test_setup
test_setup:
	docker compose up -d && env `cat .env` go run ./tool/main.go
