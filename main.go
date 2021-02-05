package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"time"

	"golang.org/x/xerrors"

	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/lotus/api"
	"github.com/filecoin-project/lotus/build"
	"github.com/filecoin-project/lotus/chain/store"
	"github.com/filecoin-project/lotus/chain/types"
	lcli "github.com/filecoin-project/lotus/cli"
	"github.com/filecoin-project/lotus/lib/lotuslog"

	logging "github.com/ipfs/go-log"

	"github.com/labstack/echo/v4"
	"github.com/urfave/cli/v2"

	_ "github.com/mattn/go-sqlite3"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

var log = logging.Logger("indexer")

type Message struct {
	gorm.Model
	Cid        string
	To         string `gorm:"index"`
	From       string `gorm:"index"`
	InclHeight uint64
	InclTsID   uint

	ReceiptID uint
}

type Receipt struct {
	gorm.Model
	Msg Message

	ExitCode int64
	Return   []byte
	GasUsed  int64
}

type TipSet struct {
	gorm.Model
	Key       string
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

	dbts := &TipSet{
		Key:       ts.Key().String(),
		Processed: true,
	}
	if err := ix.db.Create(dbts).Error; err != nil {
		return xerrors.Errorf("inserting new tipset into database failed: %w", err)
	}

	//msgs := make([]Message, 0, len(pmsgs))
	recpts := make([]Receipt, 0, len(precpts))
	for i, m := range pmsgs {
		msg := Message{
			Cid:        m.Cid.String(),
			To:         m.Message.To.String(),
			From:       m.Message.From.String(),
			InclHeight: uint64(ts.Height()),
			InclTsID:   dbts.ID,
		}

		rec := Receipt{
			Msg: msg,

			ExitCode: int64(precpts[i].ExitCode),
			Return:   precpts[i].Return,
			GasUsed:  precpts[i].GasUsed,
		}

		recpts = append(recpts, rec)
	}

	/*
		if err := ix.db.Create(msgs).Error; err != nil {
			return xerrors.Errorf("inserting new messages into database failed: %w", err)
		}
	*/

	if err := ix.db.Create(recpts).Error; err != nil {
		return xerrors.Errorf("inserting new receipts into database failed: %w", err)
	}

	return nil
}

func (ix *Indexer) indexedTipSet(ts *types.TipSet) (bool, error) {
	var count int64
	if err := ix.db.Model(&TipSet{}).Where("key = ?", ts.Key().String()).Count(&count).Error; err != nil {
		return false, err
	}

	return count >= 1, nil
}

func (ix *Indexer) crawlBack(ctx context.Context, cur *types.TipSet) error {
	for cur.Height() > 0 {
		start := time.Now()
		done, err := ix.indexedTipSet(cur)
		if err != nil {
			return err
		}
		if !done {
			if err := ix.processTipSet(ctx, cur); err != nil {
				return err
			}
		} else {
			//return nil
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

func (ix *Indexer) clearTipSet(ts *types.TipSet) error {
	var tipset TipSet
	if err := ix.db.Where("key = ?", ts.Key().String()).First(&tipset).Error; err != nil {
		if xerrors.Is(err, gorm.ErrRecordNotFound) {
			return nil
		}
		return xerrors.Errorf("failed to find entry for tipset in db: %w", err)
	}

	if err := ix.db.Where("incl_ts_id = ?", tipset.ID).Delete(&Message{}).Error; err != nil {
		return xerrors.Errorf("failed to delete messages in clear tipset: %w", err)
	}

	if err := ix.db.Where("incl_ts_id = ?", tipset.ID).Delete(&Receipt{}).Error; err != nil {
		return xerrors.Errorf("failed to delete receipts in clear tipset: %w", err)
	}

	if err := ix.db.Where("id = ?", tipset.ID).Delete(&TipSet{}).Error; err != nil {
		return xerrors.Errorf("failed to delete tipset marker in clear tipset: %w", err)
	}

	return nil
}

type APIReceipt struct {
	ExitCode int64  `json:"exit_code"`
	Return   []byte `json:"return"`
	GasUsed  int64  `json:"gas_used"`
}

type APIMessage struct {
	Cid  string `json:"cid"`
	From string `json:"from"`
	To   string `json:"to"`

	InclHeight uint64 `json:"incl_height"`

	Receipt *APIReceipt `json:"receipt,omitempty"`
}

func (ix *Indexer) MessagesCount() (int64, error) {
	var count int64
	if err := ix.db.Model(&Message{}).Count(&count).Error; err != nil {
		return 0, xerrors.Errorf("failed to find messages to target: %w", err)
	}

	return count, nil
}

func (ix *Indexer) MessagesFor(addr address.Address, limit int) ([]APIMessage, error) {

	/*
		var messages []Message
		if err := ix.db.Limit(limit).Order("incl_height desc").Where("`to` = ? OR `from` = ?", addr.String(), addr.String()).Find(&messages).Error; err != nil {
			return nil, xerrors.Errorf("failed to find messages to target: %w", err)
		}
	*/

	type Result struct {
		Message
		Receipt
	}
	var results []Result
	txn := ix.db.Model(&Message{}).
		Limit(limit).
		Order("incl_height desc").
		Where("`to` = ? OR `from` = ?", addr.String(), addr.String()).
		Select("messages.*, receipts.*").
		Joins("left join receipts on receipts.id = messages.receipt_id").
		Find(&results)
	if err := txn.Error; err != nil {
		return nil, xerrors.Errorf("messages for address query failed: %w", err)
	}
	// SELECT users.name, emails.email FROM `users` left join emails on emails.user_id = users.id

	out := make([]APIMessage, 0, len(results))
	for _, r := range results {
		out = append(out, APIMessage{
			Cid:        r.Cid,
			From:       r.From,
			To:         r.To,
			InclHeight: r.InclHeight,
			Receipt: &APIReceipt{
				ExitCode: r.Receipt.ExitCode,
				Return:   r.Receipt.Return,
				GasUsed:  r.Receipt.GasUsed,
			},
		})
	}

	return out, nil
}

func (ix *Indexer) MessagesTo(addr address.Address) ([]APIMessage, error) {
	var messages []Message
	if err := ix.db.Order("incl_height desc").Where("`to` = ?", addr.String()).Find(&messages).Error; err != nil {
		return nil, xerrors.Errorf("failed to find messages to target: %w", err)
	}

	out := make([]APIMessage, 0, len(messages))
	for _, m := range messages {
		out = append(out, APIMessage{
			Cid:        m.Cid,
			From:       m.From,
			To:         m.To,
			InclHeight: m.InclHeight,
		})
	}

	return out, nil
}

func (ix *Indexer) MessagesFrom(addr address.Address) ([]APIMessage, error) {
	var messages []Message
	if err := ix.db.Order("incl_height desc").Where("`from` = ?", addr.String()).Find(&messages).Error; err != nil {
		return nil, xerrors.Errorf("failed to find messages to target: %w", err)
	}

	out := make([]APIMessage, 0, len(messages))
	for _, m := range messages {
		out = append(out, APIMessage{
			Cid:        m.Cid,
			From:       m.From,
			To:         m.To,
			InclHeight: m.InclHeight,
		})
	}

	return out, nil
}

func (ix *Indexer) Run(ctx context.Context) error {
	sub, err := ix.api.ChainNotify(ctx)
	if err != nil {
		return err
	}

	first := <-sub

	curts := first[0].Val

	go func() {
		if err := ix.crawlBack(ctx, curts); err != nil {
			log.Errorf("failed to crawl chain back: %s", err)
		}
	}()

	hcc := store.NewHeadChangeCoalescer(func(rev, app []*types.TipSet) error {
		for _, ts := range rev {
			fmt.Printf("Revert: %s\n", ts.Key())

			if err := ix.clearTipSet(ts); err != nil {
				log.Errorf("handling reverted tipset: %w", err)
			}
		}

		for _, ts := range app {
			fmt.Printf("Apply: %s\n", ts.Key())
			start := time.Now()
			if err := ix.processTipSet(ctx, ts); err != nil {
				return xerrors.Errorf("failed to process tipset: %w", err)
			}
			fmt.Printf("processing tipset %d took: %s\n", ts.Height(), time.Since(start))
		}

		return nil
	}, time.Second*10, time.Second*30, time.Second*5)

	go func() {
		for hc := range sub {
			var rev, app []*types.TipSet
			for _, upd := range hc {
				switch upd.Type {
				case "revert":
					rev = append(rev, upd.Val)
				case "apply":
					app = append(app, upd.Val)
				}
			}
			if err := hcc.HeadChange(rev, app); err != nil {
				log.Errorf("head change failed: %w", err)
			}
		}
	}()

	return nil
}

func main() {
	lotuslog.SetupLogLevels()

	local := []*cli.Command{
		runCmd,
	}

	app := &cli.App{
		Name:    "lotus-indexer",
		Usage:   "Simple message indexer for lotus",
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
			Value: "0.0.0.0:2347",
		},
		&cli.IntFlag{
			Name:  "no-index-before",
			Usage: "dont index tipsets before the given height",
			Value: 350000,
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

		if err := ix.Run(ctx); err != nil {
			return err
		}

		e := echo.New()
		e.GET("/index/msgs/to/:addr", func(c echo.Context) error {
			addr := c.Param("addr")
			a, err := address.NewFromString(addr)
			if err != nil {
				return err
			}

			msgs, err := ix.MessagesTo(a)
			if err != nil {
				return err
			}

			return c.JSON(http.StatusOK, msgs)
		})

		e.GET("/index/msgs/from/:addr", func(c echo.Context) error {
			addr := c.Param("addr")
			a, err := address.NewFromString(addr)
			if err != nil {
				return err
			}

			msgs, err := ix.MessagesFrom(a)
			if err != nil {
				return err
			}

			return c.JSON(http.StatusOK, msgs)
		})

		e.GET("/index/msgs/for/:addr", func(c echo.Context) error {
			addr := c.Param("addr")
			a, err := address.NewFromString(addr)
			if err != nil {
				return err
			}

			msgs, err := ix.MessagesFor(a, 200)
			if err != nil {
				return err
			}

			return c.JSON(http.StatusOK, msgs)
		})

		e.GET("/index/msgs/count", func(c echo.Context) error {
			count, err := ix.MessagesCount()
			if err != nil {
				return err
			}

			return c.JSON(http.StatusOK, map[string]interface{}{
				"num_messages": count,
			})
		})

		go func() {
			<-ctx.Done()
			log.Warn("Shutting down...")

			if err := e.Close(); err != nil {
				log.Errorf("shutting down indexer server failed: %s", err)
			} else {
				log.Warn("Graceful shutdown successful")
			}
		}()

		address := cctx.String("listen")
		return e.Start(address)
	},
}
