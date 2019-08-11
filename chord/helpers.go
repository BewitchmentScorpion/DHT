package DHT

import (
	"crypto/sha1"
	"math/big"
	"net"
)

func get_hash(k string) *big.Int {
	h := sha1.New()
	h.Write([]byte(k))
  res := h.Sum(nil)
	var hash big.Int
	hash.SetBytes(res)
	return &hash
}

func copyInfo(t T_info) T_info {
	return T_info{t.addr, new(big.Int).Set(t.node_id)}
}

func get_now_addr() string {
  var res string
	ifaces, err := net.Interfaces()
	if err != nil {
		panic("init: failed to find network interfaces")
	}
	for _, opt := range ifaces {
		if opt.Flags & net.FlagLoopback == 0 && opt.Flags & net.FlagUp != 0 {
			addrs, err := opt.Addrs()
			if err != nil {
				panic("init: failed to get addresses for network interface")
			}
			for _, addr := range addrs {
				if ip_net, flag := addr.(*net.IPNet); flag {
					if ip4 := ip_net.IP.To4(); len(ip4) == net.IPv4len {
						res = ip4.String()
						break
					}
				}
			}
		}
	}
	if res == "" {
		panic("init: failed to find non-loopback interface with valid address on this node")
	}
	return res
}
