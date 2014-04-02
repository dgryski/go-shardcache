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
	deletekeys := flag.Bool("d", false, "delete keys instead of writing")
	getIndex := flag.Bool("idx", false, "query shardcache for keys to use instead of generating random keys")

	flag.Parse()

	hosts := strings.Split(*host, ",")
	nhosts := len(hosts)

	var keys [][]byte

	client, err := shardcache.New(hosts[0], nil)
	if err != nil {
		log.Fatal("unable to contact ", hosts[0], ": ", err)
	}

	if *getIndex {

		directory, err := client.Index()

		if err != nil {
			log.Fatal("failed to fetch index: ", err)
		}

		var k [][]byte

		for _, v := range directory {
			k = append(k, v.Key)
		}

		if *nkey == 0 {
			*nkey = len(k)
			keys = k
		} else {

			if *nkey < len(k) {
				*nkey = len(k)
			}

			for _, v := range rand.Perm(*nkey) {
				keys = append(keys, k[v])
			}
		}

	} else {
		// construct our 'random' keys
		for i := int64(0); i < int64(*nkey); i++ {
			keys = append(keys, strconv.AppendInt([]byte(*keystr), i, 10))
		}

		// make sure they exist
		for _, k := range keys {
			if *verbose {
				log.Println("init key", k)
			}
			client.Set(k, []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}, 0)
		}
	}

	val := new(uint64)

	for i := 0; i < *clients; i++ {

		go func() {
			var clients []*shardcache.Client
			for _, h := range hosts {
				client, err := shardcache.New(h, nil)

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

					if *deletekeys {

						if *verbose {
							log.Println("DEL client=", hosts[clientNumber], "key=", key)
						}

						err := client.Delete(key)

						if err != nil {
							log.Println("error during delete: ", err)
						}

					} else {
						// set

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
