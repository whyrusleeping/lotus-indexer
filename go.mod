module github.com/whyrusleeping/lotus-index

go 1.15

require (
	github.com/filecoin-project/go-address v0.0.5
	github.com/filecoin-project/lotus v1.4.1
	github.com/gorilla/mux v1.8.0
	github.com/ipfs/go-cid v0.0.7
	github.com/ipfs/go-log v1.0.4
	github.com/mattn/go-sqlite3 v1.14.6
	github.com/urfave/cli/v2 v2.3.0
	golang.org/x/xerrors v0.0.0-20200804184101-5ec99f83aff1
	gorm.io/driver/sqlite v1.1.4
	gorm.io/gorm v1.20.12
)

replace github.com/filecoin-project/lotus => ../lotus

replace github.com/filecoin-project/filecoin-ffi => ../lotus/extern/filecoin-ffi
