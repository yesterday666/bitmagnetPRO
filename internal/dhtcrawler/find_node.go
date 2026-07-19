package dhtcrawler

import (
	"context"
	"fmt"
	"time"

	"github.com/bitmagnet-io/bitmagnet/internal/protocol/dht/ktable"
)

func (c *crawler) getNodesForFindNode(ctx context.Context) {
	for {
		peers := c.kTable.GetOldestNodes(time.Now().Add(-(5 * time.Second)), 10)
		for _, p := range peers {
			select {
			case <-ctx.Done():
				return
			case c.nodesForFindNode.In() <- p:
				continue
			}
		}

		<-time.After(time.Second)
	}
}

func (c *crawler) runFindNode(ctx context.Context) {
	_ = c.nodesForFindNode.Run(ctx, func(p ktable.Node) {
		res, err := c.client.FindNode(ctx, p.Addr(), c.soughtNodeID.Get())
		if err != nil {
			c.kTable.BatchCommand(ktable.DropNode{
				ID:     p.ID(),
				Reason: fmt.Errorf("find_node failed: %w", err),
			})
		} else {
			c.kTable.BatchCommand(ktable.PutNode{
				ID:      p.ID(),
				Addr:    p.Addr(),
				Options: []ktable.NodeOption{ktable.NodeResponded()},
			})
			// Add all discovered IPv4 nodes
			for _, n := range res.Nodes {
				select {
				case <-ctx.Done():
					return
				case c.discoveredNodes.In() <- ktable.NewNode(n.ID, n.Addr):
					continue
				}
			}
			// Add all discovered IPv6 nodes
			for _, n := range res.Nodes6 {
				select {
				case <-ctx.Done():
					return
				case c.discoveredNodes.In() <- ktable.NewNode(n.ID, n.Addr):
					continue
				}
			}
		}
	})
}
