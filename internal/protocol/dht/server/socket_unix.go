//go:build !windows

package server

import (
	"errors"
	"fmt"
	"sync"
	"net/netip"
	"golang.org/x/sys/unix"
)

func newSocket() Socket {
	v4fd, err4 := unix.Socket(unix.AF_INET, unix.SOCK_DGRAM, 0)
	if err4 != nil {
		panic(fmt.Errorf("error creating IPv4 socket: %w", err4))
	}
	v6fd, err6 := unix.Socket(unix.AF_INET6, unix.SOCK_DGRAM, 0)
	if err6 != nil {
		unix.Close(v4fd)
		panic(fmt.Errorf("error creating IPv6 socket: %w", err6))
	}
	_ = unix.SetsockoptInt(v6fd, unix.IPPROTO_IPV6, unix.IPV6_V6ONLY, 1)
	ds := &dualSocket{
		v4fd:    v4fd,
		v6fd:    v6fd,
		packets: make(chan packet, 256),
	}
	return ds
}

type packet struct {
	data []byte
	addr netip.AddrPort
	err  error
}

type dualSocket struct {
	v4fd, v6fd int
	packets    chan packet
	wg         sync.WaitGroup
}

func (ds *dualSocket) Open(localAddr netip.AddrPort) error {
	port := localAddr.Port()
	s4Addr := &unix.SockaddrInet4{Port: int(port)}
	v4Addr := netip.AddrPortFrom(netip.IPv4Unspecified(), port)
	a4 := v4Addr.Addr().As4()
	copy(s4Addr.Addr[:], a4[:])
	if err := unix.Bind(ds.v4fd, s4Addr); err != nil {
		return fmt.Errorf("bind IPv4: %w", err)
	}
	s6Addr := &unix.SockaddrInet6{Port: int(port)}
	_ = unix.SetsockoptInt(ds.v6fd, unix.IPPROTO_IPV6, unix.IPV6_V6ONLY, 1)
	v6Addr := netip.AddrPortFrom(netip.IPv6Unspecified(), port)
	a16 := v6Addr.Addr().As16()
	copy(s6Addr.Addr[:], a16[:])
	if err := unix.Bind(ds.v6fd, s6Addr); err != nil {
		unix.Close(ds.v4fd)
		return fmt.Errorf("bind IPv6: %w", err)
	}
	ds.wg.Add(2)
	go ds.readLoop(ds.v4fd)
	go ds.readLoop(ds.v6fd)
	return nil
}

func (ds *dualSocket) readLoop(fd int) {
	defer ds.wg.Done()
	for {
		buf := make([]byte, 65536)
		n, sAddr, err := unix.Recvfrom(fd, buf, 0)
		if err != nil {
			ds.packets <- packet{err: err}
			return
		}
		addr, addrErr := sockaddrToAddrPort(sAddr)
		data := make([]byte, n)
		copy(data, buf[:n])
		ds.packets <- packet{data: data, addr: addr, err: addrErr}
	}
}

func (ds *dualSocket) Receive(data []byte) (int, netip.AddrPort, error) {
	p := <-ds.packets
	if p.err != nil {
		return 0, netip.AddrPort{}, p.err
	}
	n := copy(data, p.data)
	return n, p.addr, nil
}

func (ds *dualSocket) Close() error {
	err4 := unix.Close(ds.v4fd)
	err6 := unix.Close(ds.v6fd)
	ds.wg.Wait()
	close(ds.packets)
	return errors.Join(err4, err6)
}

func (ds *dualSocket) Send(remoteAddr netip.AddrPort, data []byte) error {
	sAddr, addrErr := addrPortToSockaddr(remoteAddr)
	if addrErr != nil {
		return addrErr
	}
	// Unmap before checking: IPv4-mapped IPv6 addresses (::ffff:x.x.x.x)
	// must be treated as IPv4 to avoid "address family not supported" errors.
	ip := remoteAddr.Addr().Unmap()
	if ip.Is4() {
		return unix.Sendto(ds.v4fd, data, 0, sAddr)
	}
	return unix.Sendto(ds.v6fd, data, 0, sAddr)
}

func addrPortToSockaddr(addr netip.AddrPort) (unix.Sockaddr, error) {
	ip := addr.Addr().Unmap()
	if ip.Is4() {
		return &unix.SockaddrInet4{Addr: ip.As4(), Port: int(addr.Port())}, nil
	}
	if ip.Is6() {
		return &unix.SockaddrInet6{Addr: ip.As16(), Port: int(addr.Port())}, nil
	}
	return nil, errors.New("invalid address")
}

func sockaddrToAddrPort(addr unix.Sockaddr) (netip.AddrPort, error) {
	switch addr := addr.(type) {
	case *unix.SockaddrInet4:
		return netip.AddrPortFrom(netip.AddrFrom4(addr.Addr), uint16(addr.Port)), nil
	case *unix.SockaddrInet6:
		return netip.AddrPortFrom(netip.AddrFrom16(addr.Addr), uint16(addr.Port)), nil
	default:
		return netip.AddrPort{}, fmt.Errorf("unsupported sockaddr type: %T", addr)
	}
}
