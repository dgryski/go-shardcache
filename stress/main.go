package main

import (
	"encoding/binary"
	"flag"
	"log"
	"math/rand"
	"strconv"
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

	var keys [][]byte

	for i := int64(0); i < int64(*nkey); i++ {
		keys = append(keys, strconv.AppendInt([]byte(*keystr), i, 10))
	}

	if *verbose {
		log.Println("keys=", keys)
	}

	val := new(uint64)

	// make sure our keys exist
	client, _ := shardcache.New(*host)
	for _, k := range keys {
		if *verbose {
			log.Println("init key", k)
		}
		client.Set(k, []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}, 0)
	}

	for i := 0; i < *clients; i++ {

		go func() {
			client, err := shardcache.New(*host)

			if err != nil {
				log.Fatal("unable to create client:", err)
			}

			rnd := rand.New(rand.NewSource(time.Now().UnixNano() + rand.Int63()))
			done := false
			for !done {
				key := keys[rnd.Intn(*nkey)]

				if *verbose {
					log.Println("key=", key)
				}

				if rnd.Intn(100) < *write {
					var v [16]byte
					vint := atomic.AddUint64(val, 1)
					binary.BigEndian.PutUint64(v[:], vint)
					err := client.Set(key, v[:], 0)
					if err != nil {
						log.Println("error during set: ", err)
					}
				} else {
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
