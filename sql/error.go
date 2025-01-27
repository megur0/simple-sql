package sql

import "errors"

var (
	PanicPlaceHolderNumberNotMatch  = "the number of PlaceHolder must match the number of args"
	PanicDeleteSQLMustUseWhere      = "delete sql must use where keyword"
	PanicSelectSQLMustUseWhere      = "select sql must use where keyword"
	PanicUpdateSQLMustUseWhere      = "update sql must use where keyword"
	PanicUpdateSQLMustHaveUpdatedAt = "update sql must have updated_at field"
	PanicLockingReadMustUseNowait   = "locking read must use nowait"
	PanicCommitDespiteErrInTx       = "you have executed commit despite there is error in transaction"
	PanicQueryNotContanSelect       = "select does not contain select"
	PanicSQLIsSeqScan               = "sql executed by Seq Scan: %s"
)

var (
	ErrLockNotAvailable = errors.New("lock not available")
	ErrUniqConstraint   = errors.New("violate uniq constraint")
	ErrDeadLock         = errors.New("dead lock")
)

var (
	PostgresErrCodeLockNotAvailable = "55P03"
	PostgresErrCodeInvalidSyntax    = "22P02"
	PostgresErrCodeUniqConstraint   = "23505"
	PostgresErrCodeDeadLock         = "40P01"
)
