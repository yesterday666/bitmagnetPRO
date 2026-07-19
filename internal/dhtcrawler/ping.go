package dhtcrawler

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/bitmagnet-io/bitmagnet/internal/protocol"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol/dht/ktable"
)

func (c *crawler) runPing(ctx context.Context) {
	_ = c.nodesForPing.Run(ctx, func(n ktable.Node) {
		if n.Dropped() || n.Time().After(time.Now().Add(-c.oldPeerThreshold)) {
			return
		}
		res, err := c.client.Ping(ctx, n.Addr())
		var nodeID protocol.ID
		if err == nil {
			nodeID = res.ID
			if !n.ID().IsZero() && n.ID() != nodeID {
				nodeID = n.ID()
				err = errors.New("node responded with a mismatching ID")
			}
		}
		if err != nil {
			c.kTable.BatchCommand(ktable.DropNode{ID: nodeID, Reason: fmt.Errorf("failed to respond to ping: %w", err)})
		} else {
			c.kTable.BatchCommand(ktable.PutNode{ID: nodeID, Addr: n.Addr(), Options: []ktable.NodeOption{ktable.NodeResponded()}})
		}
	})
}

func (c *crawler) getOldNodes(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(c.getOldestNodesInterval):
			for _, p := range c.kTable.GetOldestNodes(time.Now().Add(-c.oldPeerThreshold), 0) {
				select {
				case <-ctx.Done():
					return
				case c.nodesForPing.In() <- p:
					continue
				}
			}
		}
	}
}
