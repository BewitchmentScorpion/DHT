package DHT

import (
	"errors"
	"math/big"
	"math/rand"
	"net"
	"net/rpc"
	"sync"
	"time"
)

var pow_2_160 = new(big.Int).Exp(big.NewInt(2), big.NewInt(160), nil)

type chord_node struct {
	finger[160] T_info
	suc[160] T_info
	pre T_info
	info T_info
	data map[string] KV
	server *rpc.Server
	mutex sync.Mutex
	opt int
	listener net.Listener
	wait_list sync.WaitGroup
}

type KV struct {
	key string
	val string
}

type T_info struct {
	addr  string
	node_id *big.Int
}

func (rhs *chord_node) get_info(tmp *int, res *T_info) error {
	*res = copyInfo(rhs.info)
	return nil
}

func (rhs *chord_node) get_suc(tmp *int, res *[160]T_info) error {
	*res = rhs.suc
	return nil
}

func (rhs *chord_node) get_pre(tmp *int, res *T_info) error {
	if rhs.pre.node_id.Cmp(big.NewInt(0)) != 0 && !rhs.Ping(rhs.pre.addr) {
		rhs.pre = T_info{"", big.NewInt(0)}
	}
	*res = copyInfo(rhs.pre)
	return nil
}

func (rhs *chord_node) modify_pre(now *T_info, res *int) error {
	rhs.mutex.Lock()
	rhs.pre = copyInfo(*now)
	rhs.mutex.Unlock()
	return nil
}

func (rhs *chord_node) modify_suc(now *T_info, _ *int) error {
	if now.node_id.Cmp(rhs.info.node_id) == 0 {
		return nil
	}
	client, err := rhs.Connect(*now)
	if err != nil {
		return err
	}
	rhs.mutex.Lock()
	var T_Succ [160]T_info
	err = client.Call("Node.Get_Successors", 0, &T_Succ)
	if err != nil {
		return err
	}
	_ = client.Close()
	rhs.finger[0], rhs.suc[0] = copyInfo(*now), copyInfo(*now)
	for i := 1; i < 160; i++ {
		rhs.suc[i] = T_Succ[i-1]
	}
	rhs.mutex.Unlock()
	return nil
}

func (rhs *chord_node) get_first_suc(tmp *int, res *T_info) error {
	rhs.mutex.Lock()
	for i, t_node := range rhs.suc {
		if !rhs.Ping(t_node.addr) {
			rhs.suc[i] = T_info{"", big.NewInt(0)}
			continue
		}
		*res = copyInfo(t_node)
		rhs.mutex.Unlock()
		return nil
	}
	rhs.mutex.Unlock()
	return errors.New("not found")
}

func (rhs *chord_node) find_pre(now_id *big.Int) T_info {
	var tmp int
	var t_succ T_info
	err := rhs.get_first_suc(&tmp, &t_succ)
	if err != nil {
		return T_info{"", big.NewInt(0)}
	}
	cnt := 0
  pre := copyInfo(rhs.info)
	for (!checkBetween(big.NewInt(1).Add(pre.node_id, big.NewInt(1)), t_succ.node_id, now_id)) && cnt <= 32 {
		cnt++
		if pre.node_id.Cmp(rhs.info.node_id) != 0 {
			client, err := rhs.Connect(pre)
			if err != nil {
				return T_info{"", big.NewInt(0)}
			}
			err = client.Call("Node.Closest_Pre_Node", now_id, &pre)
			if err != nil {
				_ = client.Close()
				return T_info{"", big.NewInt(0)}
			}
			_ = client.Close()
		} else {
			err = rhs.closest_pre_node(now_id, &pre)
		}
		if err != nil {
			return T_info{"", big.NewInt(0)}
		}
		client, err := rhs.Connect(pre)
		if err != nil {
			return T_info{"", big.NewInt(0)}
		}
		err = client.Call("Node.find_first_suc", 0, &t_succ)
		if err != nil {
			return T_info{"", big.NewInt(0)}
		}
		err = client.Close()
	}
	return pre
}

func (rhs *chord_node) closest_pre_node(id *big.Int, res *T_info) error {
	for i := 159; i >= 0; i-- {
		if checkBetween(big.NewInt(1).Add(rhs.info.node_id, big.NewInt(1)), id, rhs.finger[i].node_id) {
			if !rhs.Ping(rhs.finger[i].addr) {
				rhs.finger[i] = T_info{"", big.NewInt(0)}
				continue
			}
			*res = copyInfo(rhs.finger[i])
			return nil
		}
	}
	for i := 159; i >= 0; i-- {
		if checkBetween(big.NewInt(1).Add(rhs.info.node_id, big.NewInt(1)), id, rhs.suc[i].node_id) {
			if !rhs.Ping(rhs.suc[i].addr) {
				rhs.suc[i] = T_info{"", big.NewInt(0)}
				continue
			}
			*res = copyInfo(rhs.suc[i])
			return nil
		}
	}
	*res = copyInfo(rhs.info)
	return nil
}

func (rhs *chord_node) find_suc(id *big.Int, res *T_info) error {
	pre := rhs.find_pre(id)
	client, err := rhs.Connect(pre)
	if err != nil {
		return err
	}
	err = client.Call("Node.find_first_suc", 0, res)
	_ = client.Close()
	return err
}

func (rhs *chord_node) _Get(key *string, res *string) error {
	hash_tmp := get_hash(*key)
	rhs.mutex.Lock()
	val, tmp := rhs.data[hash_tmp.String()]
	rhs.mutex.Unlock()
	if !tmp {
		return errors.New("Can't get")
	}
	*res = val.val
	return nil
}

func (rhs *chord_node) get_val(key *string, res *string) error {
	hash_tmp := get_hash(*key)
	if val, ok := rhs.data[hash_tmp.String()]; ok {
		*res = val.val
		return nil
	}
	var succ T_info
	err := rhs.find_suc(hash_tmp, &succ)
	if err != nil {
		return err
	}
	if succ.node_id.Cmp(rhs.info.node_id) != 0 {
		client, err := rhs.Connect(succ)
		if err != nil {
			return err
		}
		var _res string
		err = client.Call("Node.Get", key, &_res)
		if err != nil {
			_ = client.Close()
			return err
		}
		_ = client.Close()
		*res = _res
	}
	return nil
}

func (rhs *chord_node) _Put(kv *KV, res *bool) error {
	tmp := get_hash(kv.key)
	rhs.mutex.Lock()
	rhs.data[tmp.String()] = *kv
	rhs.mutex.Unlock()
	*res = true
	return nil
}

func (rhs *chord_node) put_val(kv *KV, res *bool) error {
	tmp := get_hash(kv.key)
	var suc T_info
	err := rhs.find_suc(tmp, &suc)
	if err != nil {
		return err
	}
	var succ T_info
	if suc.node_id.Cmp(rhs.info.node_id) == 0 {
		rhs.data[tmp.String()] = *kv
		*res = true
		succ = rhs.suc[0]
	} else {
		client, err := rhs.Connect(suc)
		if err != nil {
			return err
		}
		err = client.Call("Node.Put", kv, res)
		if err != nil {
			_ = client.Close()
			return err
		}
		_ = client.Call("Node.find_first_suc", 0, &succ)
		_ = client.Close()
	}
	client, err := rhs.Connect(succ)
	if err != nil {
		return err
	}
	var _tmp bool
	_ = client.Call("Node.Put", kv, &_tmp)
	_ = client.Close()
	return nil
}

func (rhs *chord_node) _Del(key *string, res *bool) error {
	rhs.mutex.Lock()
	hash_tmp := get_hash(*key)
	_, ok := rhs.data[hash_tmp.String()]
	if ok {
		delete(rhs.data, hash_tmp.String())
	}
	*res = ok
	rhs.mutex.Unlock()
	return nil
}

func (rhs *chord_node) del_val(key *string, res *bool) error {
	hash_tmp := get_hash(*key)
	var suc T_info
	err := rhs.find_suc(hash_tmp, &suc)
	if err != nil {
		return err
	}
	if suc.node_id.Cmp(rhs.info.node_id) == 0 {
		_, ok := rhs.data[hash_tmp.String()]
		if ok {
			delete(rhs.data, hash_tmp.String())
		}
		*res = ok
	} else {
		client, err := rhs.Connect(suc)
		if err != nil {
			return err
		}
		err = client.Call("Node.DirectDel", key, res)
		if err != nil {
			_ = client.Close()
			return err
		}
		_ = client.Close()
	}
	return nil
}

func (rhs *chord_node) trans_data(trans *T_info, res *int) error {
	if trans.addr == "" {
		return nil
	}
	client, err := rhs.Connect(*trans)
	if err != nil {
		return err
	}
	rhs.mutex.Lock()
	for hash_key, KV := range rhs.data {
		var t big.Int
		t.SetString(hash_key, 10)
		if checkBetween(rhs.info.node_id, trans.node_id, &t) {
			var tmp bool
			err := client.Call("Node.Put", &KV, &tmp)
			if err != nil {
				rhs.mutex.Unlock()
				_ = client.Close()
				return err
			}
			delete(rhs.data, hash_key)
		}
	}
	rhs.mutex.Unlock()
	_ = client.Close()
	return nil
}

func (rhs *chord_node) trans_data_force(trans *T_info, res *int) error {
	client, err := rhs.Connect(*trans)
	if err != nil {
		return err
	}
	rhs.mutex.Lock()
	for _, KV := range rhs.data {
		var tmp bool
		err := client.Call("Node.Put", &KV, &tmp)
		if err != nil {
			rhs.mutex.Unlock()
			_ = client.Close()
			return err
		}
	}
	rhs.mutex.Unlock()
	_ = client.Close()
	return nil
}

func (rhs *chord_node) stabilize() {
	for {
		if rhs.opt == 0 {
			break
		}
		var tmp int
		var x = T_info{"", big.NewInt(0)}
		err := rhs.get_first_suc(nil, &rhs.suc[0])
		if err != nil {
			continue
		}
		client, err := rhs.Connect(rhs.suc[0])
		if err != nil {
			continue
		}
		err = client.Call("Node.get_pre", 0, &x)
		if err != nil {
			continue
		}
		rhs.mutex.Lock()
		if x.node_id.Cmp(big.NewInt(0)) != 0 && checkBetween(big.NewInt(1).Add(rhs.info.node_id, big.NewInt(1)), rhs.suc[0].node_id, x.node_id) {
			rhs.suc[0], rhs.finger[0] = copyInfo(x), copyInfo(x)
		}
		rhs.mutex.Unlock()
		_ = client.Close()

		err = rhs.modify_suc(&rhs.suc[0], &tmp)
		if err != nil {
			continue
		}
		client, err = rhs.Connect(rhs.suc[0])
		if err != nil {
			continue
		}

		err = client.Call("Node.notify", &rhs.info, &tmp)
		_ = client.Close()
		if err != nil {
			continue
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (rhs *chord_node) notify(d_node *T_info, res *int) error {
	rhs.mutex.Lock()
	if rhs.pre.addr == "" || checkBetween(big.NewInt(1).Add(rhs.pre.node_id, big.NewInt(1)), rhs.info.node_id, d_node.node_id) {
		rhs.pre = copyInfo(*d_node)
		rhs.mutex.Unlock()
		if rhs.pre.addr != rhs.info.addr {
			client, err := rhs.Connect(rhs.pre)
			if err != nil {
				return err
			}
			err = client.Call("Node.maintain", 0, nil)
			_ = client.Close()
		}
	} else {
		rhs.mutex.Unlock()
	}
	return nil
}

func (rhs *chord_node) check_pre() {
	for {
		rhs.mutex.Lock()
		if rhs.pre.addr != "" {
			if !rhs.Ping(rhs.pre.addr) {
				rhs.pre = T_info{"", big.NewInt(0)}
			}
		}
		rhs.mutex.Unlock()
		time.Sleep(100 * time.Millisecond)
	}
}

func (rhs *chord_node) fix_fingers() {
	for {
		if rhs.opt == 0 {
			break
		}
		i := rand.Intn(159) + 1
		var tmp big.Int
		tmp.Add(rhs.info.node_id, tmp.Exp(big.NewInt(2), big.NewInt(int64(i)), nil))
		if tmp.Cmp(pow_2_160) >= 0 {
			tmp.Sub(&tmp, pow_2_160)
		}

		err := rhs.find_suc(&tmp, &rhs.finger[i])
		if err != nil {
			continue
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (rhs *chord_node) maintain(_ *int, _ *int) error {
	err := rhs.get_first_suc(nil, &rhs.suc[0])
	if err != nil {
		return nil
	}

	rhs.mutex.Lock()
	tmp_T := make(map[string]KV)
	for key, val := range rhs.data {
		tmp_T[key] = val
	}
	rhs.mutex.Unlock()

	client, err := rhs.Connect(rhs.suc[0])
	if err != nil {
		return nil
	}

	for hash, kv := range tmp_T {
		tmp, _ := new(big.Int).SetString(hash, 10)
		if !checkBetween(rhs.pre.node_id, rhs.info.node_id, tmp) {
			continue
		}
		var res bool
		_ = client.Call("Node.Put", &kv, &res)
	}
	_ = client.Close()
	return nil
}

func checkBetween(a, b, mid *big.Int) bool {
	if a.Cmp(b) >= 0 {
		return checkBetween(a, pow_2_160, mid) || checkBetween(big.NewInt(0), b, mid)
	}
	return mid.Cmp(a) >= 0 && mid.Cmp(b) < 0
}

func (rhs *chord_node) Connect(other T_info) (*rpc.Client, error) {
	if other.addr == "" {
		return nil, errors.New("invalid address")
	}

	c := make(chan *rpc.Client, 1)
	var err error
	var client *rpc.Client

	go func() {
		for i := 0; i < 3; i++ {
			client, err = rpc.Dial("tcp", other.addr)
			if err == nil {
				c <- client
				return
			}
			time.Sleep(10 * time.Millisecond)
		}
		c <- nil
	}()

	select {
		case client := <-c:
			if client != nil {
				return client, nil
			} else {
				return nil, errors.New("can't connect")
			}
		case <-time.After(333 * time.Millisecond):
			if err == nil {
				err = errors.New("can't connect")
			}
			return nil, err
	}
}
