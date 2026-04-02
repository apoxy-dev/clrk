//go:build linux

package worker

import (
	"context"
	"fmt"
	"net"
	"net/netip"
	"os"
	"runtime"
	"sync/atomic"

	"github.com/vishvananda/netlink"
	"github.com/vishvananda/netns"
	"golang.org/x/sys/unix"
	ctrl "sigs.k8s.io/controller-runtime"
)

// NetNSConfig holds the network namespace and TAP device configuration
// for a single sandbox.
type NetNSConfig struct {
	ID      SandboxID
	NSName  string     // "run-<id>"
	NSPath  string     // "/run/netns/run-<id>"
	TAPName string     // TAP device name inside the netns.
	TAPFD   *os.File   // Host-side TAP fd for netstack (APO-536).
	IP      netip.Addr // Container IP (e.g., 10.200.0.2).
	GW      netip.Addr // Gateway IP (e.g., 10.200.0.1).
}

// ipAllocator assigns /30 subnets sequentially from 10.200.0.0/16.
// Subnet 1 = 10.200.0.4/30 → gw .5, ctr .6
// Subnet 2 = 10.200.0.8/30 → gw .9, ctr .10
// ...etc. Wraps into the third octet as needed.
var ipCounter atomic.Uint64

func allocateIPs() (gw netip.Addr, container netip.Addr) {
	n := uint32(ipCounter.Add(1))
	offset := n * 4 // each /30 consumes 4 addresses
	prefix := netip.AddrFrom4([4]byte{10, 200, 0, 0})
	base := prefix.As4()
	// Add offset to the 16-bit host part (octets 2-3 of the host portion).
	hostBits := uint16(base[2])<<8 | uint16(base[3])
	hostBits += uint16(offset)
	base[2] = byte(hostBits >> 8)
	base[3] = byte(hostBits)
	gw = netip.AddrFrom4([4]byte{base[0], base[1], base[2], base[3] + 1})
	container = netip.AddrFrom4([4]byte{base[0], base[1], base[2], base[3] + 2})
	return gw, container
}

// SetupNetNS creates a named network namespace with a TAP device inside it.
// Returns the host-side TAP fd for the Go netstack to consume (APO-536).
func SetupNetNS(ctx context.Context, id SandboxID) (*NetNSConfig, error) {
	log := ctrl.LoggerFrom(ctx).WithValues("sandboxID", id)

	nsName := fmt.Sprintf("run-%s", id)
	nsPath := fmt.Sprintf("/run/netns/%s", nsName)
	tapName := "tap0"

	gw, ctrIP := allocateIPs()
	log.Info("Setting up network namespace", "nsPath", nsPath, "containerIP", ctrIP, "gatewayIP", gw)

	// Save the current network namespace to restore later.
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()

	origNS, err := netns.Get()
	if err != nil {
		return nil, fmt.Errorf("getting current netns: %w", err)
	}
	defer origNS.Close()

	// Create named network namespace.
	newNS, err := netns.NewNamed(nsName)
	if err != nil {
		return nil, fmt.Errorf("creating named netns %s: %w", nsName, err)
	}
	defer newNS.Close()

	// We're now in the new netns. Create TAP device.
	tapFD, err := createTAP(tapName)
	if err != nil {
		netns.Set(origNS)
		netns.DeleteNamed(nsName)
		return nil, fmt.Errorf("creating TAP device: %w", err)
	}

	// Configure the TAP device with IP.
	tap, err := netlink.LinkByName(tapName)
	if err != nil {
		tapFD.Close()
		netns.Set(origNS)
		netns.DeleteNamed(nsName)
		return nil, fmt.Errorf("finding TAP device: %w", err)
	}

	addr := &netlink.Addr{
		IPNet: &net.IPNet{
			IP:   ctrIP.AsSlice(),
			Mask: net.CIDRMask(30, 32),
		},
	}
	if err := netlink.AddrAdd(tap, addr); err != nil {
		tapFD.Close()
		netns.Set(origNS)
		netns.DeleteNamed(nsName)
		return nil, fmt.Errorf("adding IP to TAP: %w", err)
	}

	if err := netlink.LinkSetUp(tap); err != nil {
		tapFD.Close()
		netns.Set(origNS)
		netns.DeleteNamed(nsName)
		return nil, fmt.Errorf("setting TAP up: %w", err)
	}

	// Set up loopback.
	lo, err := netlink.LinkByName("lo")
	if err == nil {
		netlink.LinkSetUp(lo)
	}

	// Add default route via gateway.
	route := &netlink.Route{
		Dst: nil, // default route
		Gw:  gw.AsSlice(),
	}
	if err := netlink.RouteAdd(route); err != nil {
		tapFD.Close()
		netns.Set(origNS)
		netns.DeleteNamed(nsName)
		return nil, fmt.Errorf("adding default route: %w", err)
	}

	// Return to original netns.
	if err := netns.Set(origNS); err != nil {
		tapFD.Close()
		netns.DeleteNamed(nsName)
		return nil, fmt.Errorf("restoring original netns: %w", err)
	}

	log.Info("Network namespace ready", "nsPath", nsPath, "tapFD", tapFD.Fd())

	return &NetNSConfig{
		ID:      id,
		NSName:  nsName,
		NSPath:  nsPath,
		TAPName: tapName,
		TAPFD:   tapFD,
		IP:      ctrIP,
		GW:      gw,
	}, nil
}

// TeardownNetNS closes the TAP fd, removes the TAP device, and deletes
// the network namespace.
func TeardownNetNS(cfg *NetNSConfig) error {
	if cfg.TAPFD != nil {
		cfg.TAPFD.Close()
	}
	if err := netns.DeleteNamed(cfg.NSName); err != nil {
		return fmt.Errorf("deleting netns %s: %w", cfg.NSName, err)
	}
	return nil
}

// createTAP creates a TAP device with IFF_TAP | IFF_NO_PI | IFF_VNET_HDR
// and returns the host-side file descriptor.
func createTAP(name string) (*os.File, error) {
	fd, err := unix.Open("/dev/net/tun", unix.O_RDWR|unix.O_CLOEXEC, 0)
	if err != nil {
		return nil, fmt.Errorf("opening /dev/net/tun: %w", err)
	}

	ifr, err := unix.NewIfreq(name)
	if err != nil {
		unix.Close(fd)
		return nil, fmt.Errorf("creating ifreq: %w", err)
	}
	ifr.SetUint16(unix.IFF_TAP | unix.IFF_NO_PI | unix.IFF_VNET_HDR)

	if err := unix.IoctlIfreq(fd, unix.TUNSETIFF, ifr); err != nil {
		unix.Close(fd)
		return nil, fmt.Errorf("TUNSETIFF: %w", err)
	}

	return os.NewFile(uintptr(fd), "/dev/net/tun"), nil
}
