package main

import (
	"context"
	"net/http"
	"os"

	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/lotus/build"
	"github.com/filecoin-project/lotus/lib/lotuslog"
	logging "github.com/ipfs/go-log"
	"github.com/labstack/echo/v4"
	"github.com/urfave/cli/v2"
	"golang.org/x/xerrors"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

var log = logging.Logger("sentinel-srv")

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

type IndexApi struct {
	db *gorm.DB
}

func (ix *IndexApi) MessagesCount() (int64, error) {
	var count int64
	if err := ix.db.Model(&Message{}).Count(&count).Error; err != nil {
		return 0, xerrors.Errorf("failed to find messages to target: %w", err)
	}

	return count, nil
}

type Message struct {
	Cid    string
	To     string
	From   string
	Height uint64

	ExitCode int64
	Return   []byte
	GasUsed  int64
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

func (ix *IndexApi) MessagesFor(addr address.Address, limit int) ([]APIMessage, error) {

	/*
		var messages []Message
		if err := ix.db.Limit(limit).Order("incl_height desc").Where("`to` = ? OR `from` = ?", addr.String(), addr.String()).Find(&messages).Error; err != nil {
			return nil, xerrors.Errorf("failed to find messages to target: %w", err)
		}
	*/

	var results []Message
	txn := ix.db.Model(&Message{}).
		Limit(limit).
		Order("height desc").
		Where("\"to\" = ? OR \"from\" = ?", addr.String(), addr.String()).
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
			InclHeight: r.Height,
			Receipt: &APIReceipt{
				ExitCode: r.ExitCode,
				Return:   r.Return,
				GasUsed:  r.GasUsed,
			},
		})
	}

	return out, nil
}

func (ix *IndexApi) MessagesTo(addr address.Address) ([]APIMessage, error) {
	var messages []Message
	if err := ix.db.Order("height desc").Where("`to` = ?", addr.String()).Find(&messages).Error; err != nil {
		return nil, xerrors.Errorf("failed to find messages to target: %w", err)
	}

	out := make([]APIMessage, 0, len(messages))
	for _, m := range messages {
		out = append(out, APIMessage{
			Cid:        m.Cid,
			From:       m.From,
			To:         m.To,
			InclHeight: m.Height,
		})
	}

	return out, nil
}

func (ix *IndexApi) MessagesFrom(addr address.Address) ([]APIMessage, error) {
	var messages []Message
	if err := ix.db.Order("height desc").Where("`from` = ?", addr.String()).Find(&messages).Error; err != nil {
		return nil, xerrors.Errorf("failed to find messages to target: %w", err)
	}

	out := make([]APIMessage, 0, len(messages))
	for _, m := range messages {
		out = append(out, APIMessage{
			Cid:        m.Cid,
			From:       m.From,
			To:         m.To,
			InclHeight: m.Height,
		})
	}

	return out, nil
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
		&cli.StringFlag{
			Name:  "postgres",
			Usage: "specify postgres database connection string",
		},
	},
	Action: func(cctx *cli.Context) error {
		log.Info("Starting sentinel message index server")

		db, err := gorm.Open(postgres.Open(cctx.String("postgres")), &gorm.Config{})
		if err != nil {
			return err
		}

		ix := &IndexApi{db}

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

		ctx := context.Background() // todo: tie into sigint handler
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
