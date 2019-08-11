package main

import (
	"./chord"
	"strconv"
)

func NewNode(port int) dhtNode {
	var Nnode DHT.chord_node
	Nnode.Create_addr(DHT.get_now_addr() + ":" + strconv.Itoa(port))
	Nnode.Create()
	return &Nnode
}
