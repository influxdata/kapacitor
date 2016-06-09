package main

import (
	"fmt"
	"log"
	"net"

	collectd "github.com/kimor79/gollectd"
)

// Listen for collectd network packets, parse , and send them over a channel
func Listen(addr string, c chan collectd.Packet, typesdb string) {
	laddr, err := net.ResolveUDPAddr("udp", addr)
	if err != nil {
		log.Fatalln("fatal: failed to resolve address", err)
	}

	conn, err := net.ListenUDP("udp", laddr)
	if err != nil {
		log.Fatalln("fatal: failed to listen", err)
	}

	types, err := collectd.TypesDBFile(typesdb)
	if err != nil {
		log.Fatalln("fatal: failed to parse types.db", err)
	}

	for {
		// 1452 is collectd 5's default buffer size. See:
		// https://collectd.org/wiki/index.php/Binary_protocol
		buf := make([]byte, 1452)

		n, err := conn.Read(buf[:])
		if err != nil {
			log.Println("error: Failed to receive packet", err)
			continue
		}

		packets, err := collectd.Packets(buf[0:n], types)
		if err != nil {
			log.Println("error: Failed to receive packet", err)
			continue
		}

		for _, p := range *packets {
			c <- p
		}
	}
}

func main() {
	c := make(chan collectd.Packet)
	go Listen("127.0.0.1:25826", c, "/usr/share/collectd/types.db")

	for {
		packet := <-c
		fmt.Printf("%+v\n", packet)
	}
}
