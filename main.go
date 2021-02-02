package main

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"time"

	"github.com/filecoin-project/go-address"
	"golang.org/x/xerrors"

	"github.com/filecoin-project/lotus/api"
	"github.com/filecoin-project/lotus/build"
	"github.com/filecoin-project/lotus/chain/types"
	lcli "github.com/filecoin-project/lotus/cli"
	"github.com/filecoin-project/lotus/lib/lotuslog"

	"github.com/ipfs/go-cid"
	logging "github.com/ipfs/go-log"

	"github.com/gorilla/mux"
	"github.com/urfave/cli/v2"

	_ "github.com/mattn/go-sqlite3"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

var log = logging.Logger("gateway")

type Message struct {
	gorm.Model
	Cid        cid.Cid
	To         address.Address
	From       address.Address
	InclHeight int64
	InclTs     types.TipSetKey
}

type Receipt struct {
	gorm.Model
	Msg    cid.Cid
	InclTs types.TipSetKey

	ExitCode int64
	Return   []byte
	GasUsed  int64
}

type TipSet struct {
	gorm.Model
	Key       types.TipSetKey
	Processed bool
}

type Indexer struct {
	api api.FullNode
	db  *gorm.DB
}

func NewIndexer(api api.FullNode, dbpath string) (*Indexer, error) {
	db, err := gorm.Open(sqlite.Open(dbpath), &gorm.Config{})
	if err != nil {
		return nil, err
	}

	if err := db.AutoMigrate(&Message{}); err != nil {
		return nil, err
	}

	if err := db.AutoMigrate(&Receipt{}); err != nil {
		return nil, err
	}

	if err := db.AutoMigrate(&TipSet{}); err != nil {
		return nil, err
	}

	return &Indexer{
		api: api,
		db:  db,
	}, nil
}

func (ix *Indexer) processTipSet(ctx context.Context, ts *types.TipSet) error {
	pmsgs, err := ix.api.ChainGetParentMessages(ctx, ts.Cids()[0])
	if err != nil {
		return err
	}

	precpts, err := ix.api.ChainGetParentReceipts(ctx, ts.Cids()[0])
	if err != nil {
		return err
	}

	tsk := ts.Key()
	msgs := make([]Message, 0, len(pmsgs))
	recpts := make([]Receipt, 0, len(precpts))
	for i, m := range pmsgs {
		msg := Message{
			Cid:        m.Cid,
			To:         m.Message.To,
			From:       m.Message.From,
			InclHeight: int64(ts.Height()),
			InclTs:     tsk,
		}

		msgs = append(msgs, msg)

		rec := Receipt{
			Msg:    m.Cid,
			InclTs: tsk,

			ExitCode: int64(precpts[i].ExitCode),
			Return:   precpts[i].Return,
			GasUsed:  precpts[i].GasUsed,
		}

		recpts = append(recpts, rec)
	}

	if err := ix.db.Create(msgs).Error; err != nil {
		return xerrors.Errorf("inserting new messages into database failed: %w", err)
	}

	if err := ix.db.Create(recpts).Error; err != nil {
		return xerrors.Errorf("inserting new receipts into database failed: %w", err)
	}

	return nil
}

func (ix *Indexer) indexedTipSet(ts *types.TipSet) (bool, error) {
	var count int64
	if err := ix.db.Model(&TipSet{}).Where("key = ?", ts.Key()).Count(&count).Error; err != nil {
		return false, err
	}

	return count >= 1, nil
}

func (ix *Indexer) crawlBack(cur *types.TipSet) error {
	ctx := context.TODO()
	for cur.Height() > 0 {
		start := time.Now()
		done, err := ix.indexedTipSet(cur)
		if err != nil {
			return err
		}
		if done {
			continue
		}

		if err := ix.processTipSet(ctx, cur); err != nil {
			return err
		}

		next, err := ix.api.ChainGetTipSet(ctx, cur.Parents())
		if err != nil {
			return err
		}

		fmt.Printf("Processing height %d took %s\n", cur.Height(), time.Since(start))
		cur = next
	}

	return nil
}

func (ix *Indexer) Run() error {
	sub, err := ix.api.ChainNotify(context.TODO())
	if err != nil {
		return err
	}

	first := <-sub

	curts := first[0].Val

	go func() {
		if err := ix.crawlBack(curts); err != nil {
			log.Errorf("failed to crawl chain back: %s", err)
		}
	}()

	for hc := range sub {
		_ = hc

	}

	return nil
}

func main() {
	lotuslog.SetupLogLevels()

	local := []*cli.Command{
		runCmd,
	}

	app := &cli.App{
		Name:    "lotus-gateway",
		Usage:   "Public API server for lotus",
		Version: build.UserVersion(),
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:    "repo",
				EnvVars: []string{"LOTUS_PATH"},
				Value:   "~/.lotus", // TODO: Consider XDG_DATA_HOME
			},
		},

		Commands: local,
	}
	app.Setup()

	if err := app.Run(os.Args); err != nil {
		log.Warnf("%+v", err)
		return
	}
}

var runCmd = &cli.Command{
	Name:  "run",
	Usage: "Start api server",
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:  "listen",
			Usage: "host address and port the api server will listen on",
			Value: "0.0.0.0:2346",
		},
	},
	Action: func(cctx *cli.Context) error {
		log.Info("Starting lotus indexer")

		ctx := lcli.ReqContext(cctx)
		ctx, cancel := context.WithCancel(ctx)
		defer cancel()

		api, closer, err := lcli.GetFullNodeAPI(cctx)
		if err != nil {
			return err
		}
		defer closer()

		ix, err := NewIndexer(api, "funtimes.db")
		if err != nil {
			return err
		}

		if err := ix.Run(); err != nil {
			return err
		}

		address := cctx.String("listen")
		mux := mux.NewRouter()

		log.Info("Setting up API endpoint at " + address)

		mux.PathPrefix("/").Handler(http.DefaultServeMux)

		/*ah := &auth.Handler{
			Verify: nodeApi.AuthVerify,
			Next:   mux.ServeHTTP,
		}*/

		srv := &http.Server{
			Handler: mux,
		}

		go func() {
			<-ctx.Done()
			log.Warn("Shutting down...")
			if err := srv.Shutdown(context.TODO()); err != nil {
				log.Errorf("shutting down indexer server failed: %s", err)
			}
			log.Warn("Graceful shutdown successful")
		}()

		nl, err := net.Listen("tcp", address)
		if err != nil {
			return err
		}

		return srv.Serve(nl)
	},
}
