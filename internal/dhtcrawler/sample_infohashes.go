package dhtcrawler

import (
	"context"
	"fmt"
	"time"

	"net/netip"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol/dht/ktable"
)

func (c *crawler) getNodesForSampleInfoHashes(ctx context.Context) {
	for {
		peers := c.kTable.GetNodesForSampleInfoHashes(60)
		for _, p := range peers {
			select {
			case <-ctx.Done():
				return
			case c.nodesForSampleInfoHashes.In() <- p:
				continue
			}
		}

		<-time.After(time.Second)
	}
}

func (c *crawler) runSampleInfoHashes(ctx context.Context) {
	_ = c.nodesForSampleInfoHashes.Run(ctx, func(n ktable.Node) {
		if !n.IsSampleInfoHashesCandidate() {
			return
		}

		res, err := c.client.SampleInfoHashes(ctx, n.Addr(), c.soughtNodeID.Get())
		if err != nil {
			c.kTable.BatchCommand(
				ktable.DropNode{ID: n.ID(), Reason: fmt.Errorf("sample_infohashes failed: %w", err)},
			)

			return
		}

		var discoveredHashes []nodeHasPeersForHash

		for _, s := range res.Samples {
			if !c.ignoreHashes.testAndAdd(s) {
				discoveredHashes = append(discoveredHashes, nodeHasPeersForHash{
					infoHash: s,
					node:     n.Addr(),
				})
			}
		}

		for _, h := range discoveredHashes {
			select {
			case <-ctx.Done():
				return
			case c.infoHashTriage.In() <- h:
				continue
			}
		}

		interval := res.Interval
		if len(discoveredHashes) > 0 && interval > 300 {
			interval = 60
		}

		c.kTable.BatchCommand(ktable.PutNode{ID: n.ID(), Addr: n.Addr(), Options: []ktable.NodeOption{
			ktable.NodeResponded(),
			ktable.NodeBep51Support(true),
			ktable.NodeSampleInfoHashesRes(
				len(discoveredHashes),
				res.Num,
				time.Now().Add(time.Duration(interval)*time.Second),
			),
		}})

		// Add discovered IPv4 AND IPv6 nodes
		allNodes := make([]struct {
			ID   [20]byte
			Addr netip.AddrPort
		}, 0, len(res.Nodes)+len(res.Nodes6))

		for _, n := range res.Nodes {
			allNodes = append(allNodes, struct {
				ID   [20]byte
				Addr netip.AddrPort
			}{n.ID, n.Addr})
		}
		for _, n := range res.Nodes6 {
			allNodes = append(allNodes, struct {
				ID   [20]byte
				Addr netip.AddrPort
			}{n.ID, n.Addr})
		}

		if len(allNodes) > 0 {
			go func() {
				timeoutCtx, cancel := context.WithTimeout(ctx, time.Second)
				defer cancel()

				for _, n := range allNodes {
					select {
					case <-timeoutCtx.Done():
						return
					case c.discoveredNodes.In() <- ktable.NewNode(n.ID, n.Addr):
						continue
					}
				}
			}()
		}
	})
}
