package DHT

import (
	"log"
	"math/big"
	"net"
	"net/rpc"
	"time"
)

func (rhs *chord_node) Get(k string) (bool, string) {
	var val string
	for i := 0; i < 5; i++ {
		_ = rhs.get_val(&k, &val)
		if val != "" {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	return val != "", val
}

func (rhs *chord_node) Put(k string, v string) bool {
	var tmp bool
	kvp := KV{k, v}
	_ = rhs.put_val(&kvp, &tmp)
	return true
}

func (rhs *chord_node) Del(k string) bool {
	var tmp bool
	_ = rhs.del_val(&k, &tmp)
	return true
}

func (rhs *chord_node) Run() {
	rhs.opt = 1
	rhs.server = rpc.NewServer()
	_ = rhs.server.Register(rhs)

	var err error = nil
	rhs.listener, err = net.Listen("tcp", rhs.info.addr)
	if err != nil {
		log.Fatal("listen error: ", err)
	}
	go rhs.server.Accept(rhs.listener)
	go rhs.stabilize()
	go rhs.fix_fingers()
	go rhs.check_pre()
}

func (rhs *chord_node) Create() {
	if len(rhs.data) == 0 {
		rhs.data = make(map[string]KV)
	}
	for i := 0; i < 160; i++ {
		rhs.finger[i], rhs.suc[i] = copyInfo(rhs.info), copyInfo(rhs.info)
	}
}

func (rhs *chord_node) Join(addr string) bool {
	client, err := rhs.Connect(T_info{addr, big.NewInt(0)})
	if err != nil {
		return false
	}
	var other T_info
	err = client.Call("Node.get_node_info", 0, &other)
	if err != nil {
		return false
	}
	rhs.mutex.Lock()
	err = client.Call("Node.find_suc", rhs.info.node_id, &rhs.suc[0])
	rhs.finger[0] = copyInfo(rhs.suc[0])
	rhs.mutex.Unlock()
	_ = client.Close()
	client, err = rhs.Connect(rhs.suc[0])
	if err != nil {
		return false
	}
	var tmp int
	err = client.Call("Node.Notify", &rhs.info, &tmp)
	if err != nil {
		return false
	}
	var pre T_info
	err = client.Call("Node.get_pre", 0, &pre)
	if err != nil {
	    return false
	}
	err = client.Call("Node.trans_data", &pre, &tmp)
	if err != nil {
		return false
	}
	_ = client.Close()
	return true
}

func (rhs *chord_node) Quit() {
	_ = rhs.listener.Close()
	rhs.opt = 0
	err := rhs.get_first_suc(nil, &rhs.suc[0])
	if err != nil {
		return
	}
	var tmp int
	err = rhs.trans_data_force(&rhs.suc[0], &tmp)
	if err != nil {
		return
	}
	client, err := rhs.Connect(rhs.pre)
	if err != nil {
		return
	}
	err = client.Call("Node.modify_suc", &rhs.suc[0], &tmp)
	_ = client.Close()
	if err != nil {
		return
	}
	client, err = rhs.Connect(rhs.suc[0])
	if err != nil {
		return
	}
	err = client.Call("Node.modify_pre", &rhs.pre, &tmp)
	_ = client.Close()
	if err != nil {
		return
	}
	rhs.clear()
}

func (rhs *chord_node) ForceQuit() {
	_ = rhs.listener.Close()
	rhs.opt = 0
}

func (rhs *chord_node) Ping(addr string) bool {
	if addr == rhs.info.addr {
		return rhs.opt > 0
	}
	client, err := rhs.Connect(T_info{addr, big.NewInt(0)})
	if err != nil {
		return false
	}
	var success int
	_ = client.Call("Node.Get_opt", 0, &success)
	_ = client.Close()
	return success > 0
}

func (rhs *chord_node) Get_opt(__ *int, res *int) error {
	*res = rhs.opt
	return nil
}

func (rhs *chord_node) Create_addr(addr string) {
	var t = get_hash(addr)
	rhs.info = T_info{addr, t}
	rhs.pre = T_info{"", big.NewInt(0)}
}

func (rhs *chord_node) Append_To(k string, v string) {
	tmp, t := rhs.Get(k)
	if tmp {
		rhs.Put(k, t + v)
	}
}

func (rhs *chord_node) clear() {
	for i := 0; i < 160; i++ {
		rhs.suc[i] = T_info{"", big.NewInt(0)}
	}
}
