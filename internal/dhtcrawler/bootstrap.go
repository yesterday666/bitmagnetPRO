package dhtcrawler

import (
	"context"
	"net"
	"net/netip"
	"time"

	"github.com/bitmagnet-io/bitmagnet/internal/protocol/dht/ktable"
)

func (c *crawler) reseedBootstrapNodes(ctx context.Context) {
	interval := time.Duration(0)

	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(interval):
			for _, strAddr := range c.bootstrapNodes {
				c.resolveAndAddNodes(ctx, strAddr)
			}
		}
		interval = c.reseedBootstrapNodesInterval
	}
}

// resolveAndAddNodes resolves a bootstrap address and adds nodes for both IPv4 and IPv6,
// preferring IPv6 to ensure the DHT routing table includes IPv6 entries.
func (c *crawler) resolveAndAddNodes(ctx context.Context, strAddr string) {
	host, port, err := net.SplitHostPort(strAddr)
	if err != nil {
		c.logger.Warnw("failed to parse bootstrap address", "addr", strAddr, "error", err)
		return
	}

	portNum, err := net.LookupPort("udp", port)
	if err != nil {
		c.logger.Warnw("failed to parse port", "port", port, "error", err)
		return
	}

	// Collect unique addresses, preferring IPv6
	var addrs []netip.Addr

	// If it's already a literal IP, use it directly
	if ip := net.ParseIP(host); ip != nil {
		if addr, ok := netip.AddrFromSlice(ip); ok {
			addrs = append(addrs, addr)
		}
	} else {
		// Resolve hostname: get both IPv4 and IPv6
		ips, lookupErr := net.DefaultResolver.LookupIPAddr(ctx, host)
		if lookupErr != nil {
			c.logger.Warnw("failed to resolve host", "host", host, "error", lookupErr)
			return
		}
		// Add IPv6 first
		for _, ip := range ips {
			if ip.IP.To4() == nil {
				if addr, ok := netip.AddrFromSlice(ip.IP); ok {
					addrs = append(addrs, addr)
				}
			}
		}
		// Then IPv4
		for _, ip := range ips {
			if ip.IP.To4() != nil {
				if addr, ok := netip.AddrFromSlice(ip.IP); ok {
					addrs = append(addrs, addr)
				}
			}
		}
	}

	if len(addrs) == 0 {
		c.logger.Warnw("no usable addresses", "addr", strAddr)
		return
	}

	for _, addr := range addrs {
		ap := netip.AddrPortFrom(addr.Unmap(), uint16(portNum))
		select {
		case <-ctx.Done():
			return
		case c.nodesForPing.In() <- ktable.NewNode(ktable.ID{}, ap):
			continue
		}
	}
}
