package dhtcrawler

import (
	"context"
	"errors"
	"fmt"
	"net/netip"
	"time"

	"github.com/bitmagnet-io/bitmagnet/internal/protocol/dht/client"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol/dht/ktable"
)

func (c *crawler) runGetPeers(ctx context.Context) {
	_ = c.getPeers.Run(ctx, func(req nodeHasPeersForHash) {
		pfh, pfhErr := c.requestPeersForHash(ctx, req)
		if pfhErr != nil {
			return
		}

		peers := make([]netip.AddrPort, 0, len(pfh.peers))
		hashPeers := make([]ktable.HashPeer, 0, len(pfh.peers))

		for _, p := range pfh.peers {
			peers = append(peers, p)
			hashPeers = append(hashPeers, ktable.HashPeer{
				Addr: p,
			})
		}

		c.kTable.BatchCommand(
			ktable.PutHash{ID: req.infoHash, Peers: hashPeers},
		)
		select {
		case <-ctx.Done():
			return
		case c.requestMetaInfo.In() <- infoHashWithPeers{
			nodeHasPeersForHash: req,
			peers:               peers,
		}:
			return
		}
	})
}

func (c *crawler) requestPeersForHash(
	ctx context.Context,
	req nodeHasPeersForHash,
) (infoHashWithPeers, error) {
	res, err := c.client.GetPeers(ctx, req.node, req.infoHash)
	if err != nil {
		c.kTable.BatchCommand(ktable.DropAddr{
			Addr:   req.node.Addr(),
			Reason: fmt.Errorf("failed to get peers: %w", err),
		})

		return infoHashWithPeers{}, err
	}

	c.kTable.BatchCommand(ktable.PutNode{
		ID:      res.ID,
		Addr:    req.node,
		Options: []ktable.NodeOption{ktable.NodeResponded()},
	})

	// block the channel for up to a second to add discovered nodes
	cancelCtx, cancel := context.WithTimeout(ctx, time.Second)

	addNodes := func(nodes []client.NodeInfo) {
		for _, n := range nodes {
			select {
			case <-cancelCtx.Done():
				return
			case c.discoveredNodes.In() <- ktable.NewNode(n.ID, n.Addr):
				continue
			}
		}
	}

	if len(res.Nodes) > 0 {
		addNodes(res.Nodes)
	}
	if len(res.Nodes6) > 0 {
		addNodes(res.Nodes6)
	}

	cancel()

	if len(res.Values) < 1 {
		return infoHashWithPeers{}, errors.New("no peers found")
	}

	return infoHashWithPeers{
		nodeHasPeersForHash: req,
		peers:               res.Values,
	}, nil
}
