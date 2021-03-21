package main

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"gorm.io/gorm"

	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/lotus/blockstore"
	market "github.com/filecoin-project/lotus/chain/actors/builtin/market"
	"github.com/filecoin-project/lotus/chain/store"
	"github.com/filecoin-project/lotus/chain/types"
	"github.com/ipfs/go-cid"
)

type DealRecord struct {
	gorm.Model
	Cid    string
	DealID int64
}

func (ix *Indexer) IndexDeals(ctx context.Context, ts *types.TipSet) error {

	bs := blockstore.NewAPIBlockstore(ix.api)
	ast := store.ActorStore(ctx, bs)

	act, err := ix.api.StateGetActor(ctx, market.Address, ts.Key())
	if err != nil {
		return err
	}

	mst, err := market.Load(ast, act)
	if err != nil {
		return err
	}

	end, err := mst.NextID()
	if err != nil {
		return err
	}

	var dr DealRecord
	ix.db.Order("deal_id desc").First(&dr)

	topVal := dr.DealID

	props, err := mst.Proposals()
	if err != nil {
		return err
	}

	for i := abi.DealID(topVal + 1); i < end; i++ {
		deal, ok, err := props.Get(i)
		if err != nil {
			return err
		}

		if !ok {
			continue
		}

		c, err := parseLabel(deal.Label)
		if err != nil {
			fmt.Println("label parsing error: ", err)
			continue
		}

		if err := ix.db.Create(&DealRecord{
			Cid:    c.String(),
			DealID: int64(i),
		}).Error; err != nil {
			return err
		}
	}

	if err != nil {
		return err
	}

	return nil
}

func parseLabel(label string) (cid.Cid, error) {
	if strings.HasPrefix(label, "{") {
		var ll legacyLabel
		if err := json.Unmarshal([]byte(label), &ll); err != nil {
			return cid.Undef, err
		}

		if len(ll.Pcids) > 0 {
			return ll.Pcids[0], nil
		} else {
			return cid.Undef, fmt.Errorf("weird format: %q", label)
		}
	} else if strings.HasPrefix(label, "m") || strings.HasPrefix(label, "Qm") {
		c, err := cid.Decode(label)
		if err != nil {
			return cid.Undef, err
		}

		return c, nil
	} else {
		return cid.Undef, fmt.Errorf("unrecognized: %q", label)
	}
}

type legacyLabel struct {
	Pcids []cid.Cid
}
