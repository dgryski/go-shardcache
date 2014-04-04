package main

import (
	"encoding/binary"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/dgryski/go-shardcache"
)

type Result struct {
	Write    bool
	Start    int64
	Duration time.Duration
	Err      error
}

type Results []Result

func (r Results) Len() int           { return len(r) }
func (r Results) Less(i, j int) bool { return r[i].Start < r[j].Start }
func (r Results) Swap(i, j int)      { r[i], r[j] = r[j], r[i] }

func main() {

	host := flag.String("h", "localhost:4444", "shardcache host")
	clients := flag.Int("c", 10, "number of concurrent clients")
	write := flag.Int("w", 50, "percentage of calls which should be writes")
	keystr := flag.String("k", "stress", "basename key to write to")
	nkey := flag.Int("nk", 10, "number of keys to set/get")
	verbose := flag.Bool("v", false, "verbose logging")
	deletekeys := flag.Bool("del", false, "delete keys instead of writing")
	getIndex := flag.Bool("idx", false, "query shardcache for keys to use instead of generating random keys")
	timeout := flag.Duration("timeout", 0, "length of time to run")
	//	rate := flag.Int("rate", 0, "rate (qps)")

	flag.Parse()

	hosts := strings.Split(*host, ",")
	nhosts := len(hosts)

	var keys [][]byte

	client, err := shardcache.New(hosts[0], nil)
	if err != nil {
		log.Fatal("unable to contact ", hosts[0], ": ", err)
	}

	if *getIndex {

		log.Println("fetching index")

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

	resultsCh := make(chan Results)
	done := make(chan struct{})

	log.Println("starting ...")

	for i := 0; i < *clients; i++ {

		go func(done <-chan struct{}, resultsCh chan<- Results) {
			var clients []*shardcache.Client
			for _, h := range hosts {
				client, err := shardcache.New(h, nil)

				if err != nil {
					log.Fatal("unable to create client for host ", h, ": ", err)
				}
				clients = append(clients, client)
			}

			results := make(Results, 0, 1000)

			rnd := rand.New(rand.NewSource(time.Now().UnixNano() + rand.Int63()))
			for {
				key := keys[rnd.Intn(*nkey)]

				clientNumber := rnd.Intn(nhosts)
				client := clients[clientNumber]

				var r Result

				if rnd.Intn(100) < *write {

					r.Write = true
					if *deletekeys {

						if *verbose {
							log.Println("DEL client=", hosts[clientNumber], "key=", key)
						}

						t0 := time.Now()
						r.Start = t0.UnixNano()
						r.Err = client.Delete(key)
						r.Duration = time.Since(t0)

						if r.Err != nil {
							log.Println("error during delete: ", r.Err)
						}

					} else {
						// set

						if *verbose {
							log.Println("SET client=", hosts[clientNumber], "key=", key)
						}

						var v [16]byte
						vint := atomic.AddUint64(val, 1)
						binary.BigEndian.PutUint64(v[:], vint)

						t0 := time.Now()
						r.Start = t0.UnixNano()
						r.Err = client.Set(key, v[:], 0)
						r.Duration = time.Since(t0)

						if r.Err != nil {
							log.Println("error during set: ", r.Err)
						}

					}

				} else {

					if *verbose {
						log.Println("GET client=", hosts[clientNumber], "key=", key)
					}

					t0 := time.Now()
					r.Start = t0.UnixNano()
					_, r.Err = client.Get(key)
					r.Duration = time.Since(t0)

					if r.Err != nil {
						log.Println("error during get: ", r.Err)
					}
				}

				results = append(results, r)

				select {
				case <-done:
					// the channel has been closed -- this is the shutdown signal
					resultsCh <- results
					return
				default:

				}
			}
		}(done, resultsCh)
	}

	c := make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGINT, syscall.SIGTERM)

	var timeoutch <-chan time.Time

	if *timeout != 0 {
		timeoutch = time.After(*timeout)
	}

	// wait for the user to kill us
	select {
	case <-c:
		log.Println("caught signal -- terminating")
	case <-timeoutch:
		log.Println("timeout -- terminating")
	}

	fname := fmt.Sprintf("loadtest-%s.out", time.Now().Format("20060102150405"))
	timingsFile, _ := os.Create(fname)
	defer timingsFile.Close()

	log.Println("writing load-test results to", fname)

	// send quit to all children
	close(done)

	jenc := json.NewEncoder(timingsFile)

	var results Results

	// and read back the responses
	for i := 0; i < *clients; i++ {
		results = append(results, <-resultsCh...)
	}

	sort.Sort(results)

	jenc.Encode(results)

}
