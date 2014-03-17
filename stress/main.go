package main

import (
	"flag"
	"log"

	"encoding/binary"
	"math/rand"
	"sync/atomic"
	"time"
	"github.com/dgryski/go-shardcache"
)

func main() {

	host := flag.String("h", "localhost:4444", "shardcache host")
	clients := flag.Int("c", 10, "number of concurrent clients")
	write := flag.Int("w", 50, "percentage of calls which should be writes")
	keystr := flag.String("k", "stress", "key to write to")

	flag.Parse()

	key := []byte(*keystr)
	val := new(uint64)
	quit := make(chan struct{})

	// make sure our key exists
	client, _ := shardcache.New(*host)
	client.Set(key, []byte("000000000000"), 0)

	for i := 0; i < *clients; i++ {

		go func() {
			client, err := shardcache.New(*host)

			if err != nil {
				log.Fatal("unable to create client:", err)
			}

			rnd := rand.New(rand.NewSource(time.Now().UnixNano() + rand.Int63()))
			done := false
			for !done {

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

				select {
				case <-quit:
					done = true
				default:

				}
			}
		}()
	}

	// wait for the user to kill us
	select {}
}
