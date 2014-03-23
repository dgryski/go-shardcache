package main

import (
	"encoding/binary"
	"flag"
	"log"
	"math/rand"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/dgryski/go-shardcache"
)

func main() {

	host := flag.String("h", "localhost:4444", "shardcache host")
	clients := flag.Int("c", 10, "number of concurrent clients")
	write := flag.Int("w", 50, "percentage of calls which should be writes")
	keystr := flag.String("k", "stress", "basename key to write to")
	nkey := flag.Int("nk", 10, "number of keys to set/get")
	verbose := flag.Bool("v", false, "verbose logging")

	flag.Parse()

	hosts := strings.Split(*host, ",")
	nhosts := len(hosts)

	var keys [][]byte

	for i := int64(0); i < int64(*nkey); i++ {
		keys = append(keys, strconv.AppendInt([]byte(*keystr), i, 10))
	}

	if *verbose {
		log.Println("keys=", keys)
	}

	val := new(uint64)

	// make sure our keys exist
	client, err := shardcache.New(hosts[0])
	if err != nil {
		log.Fatal("unable to contact ", hosts[0], ": ", err)
	}
	for _, k := range keys {
		if *verbose {
			log.Println("init key", k)
		}
		client.Set(k, []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}, 0)
	}

	for i := 0; i < *clients; i++ {

		go func() {
			var clients []*shardcache.Client
			for _, h := range hosts {
				client, err := shardcache.New(h)

				if err != nil {
					log.Fatal("unable to create client for host ", h, ": ", err)
				}
				clients = append(clients, client)
			}

			rnd := rand.New(rand.NewSource(time.Now().UnixNano() + rand.Int63()))
			done := false
			for !done {
				key := keys[rnd.Intn(*nkey)]

				clientNumber := rnd.Intn(nhosts)
				client := clients[clientNumber]

				if rnd.Intn(100) < *write {
					if *verbose {
						log.Println("SET client=", hosts[clientNumber], "key=", key)
					}

					var v [16]byte
					vint := atomic.AddUint64(val, 1)
					binary.BigEndian.PutUint64(v[:], vint)
					err := client.Set(key, v[:], 0)

					if err != nil {
						log.Println("error during set: ", err)
					}

				} else {

					if *verbose {
						log.Println("GET client=", hosts[clientNumber], "key=", key)
					}

					_, err := client.Get(key)

					if err != nil {
						log.Println("error during get: ", err)
					}
				}
			}
		}()
	}

	// wait for the user to kill us
	select {}
}
