package main

import (
	"encoding/binary"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
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
	deletekeys := flag.Bool("del", false, "delete keys instead of writing")
	getIndex := flag.Bool("idx", false, "query shardcache for keys to use instead of generating random keys")
	timeout := flag.Duration("timeout", 0, "length of time to run")

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

	type resultTimings struct {
		sets    []int
		gets    []int
		getErrs int
		setErrs int
	}

	resultsCh := make(chan resultTimings)
	done := make(chan struct{})

	for i := 0; i < *clients; i++ {

		go func(done <-chan struct{}, resultsCh chan<- resultTimings) {
			var clients []*shardcache.Client
			for _, h := range hosts {
				client, err := shardcache.New(h, nil)

				if err != nil {
					log.Fatal("unable to create client for host ", h, ": ", err)
				}
				clients = append(clients, client)
			}

			setTimings := make([]int, 0, 1000)
			getTimings := make([]int, 0, 1000)

			var getErrors int
			var setErrors int

			rnd := rand.New(rand.NewSource(time.Now().UnixNano() + rand.Int63()))
			for {
				key := keys[rnd.Intn(*nkey)]

				clientNumber := rnd.Intn(nhosts)
				client := clients[clientNumber]

				if rnd.Intn(100) < *write {

					if *deletekeys {

						if *verbose {
							log.Println("DEL client=", hosts[clientNumber], "key=", key)
						}

						t0 := time.Now()
						err := client.Delete(key)
						setTimings = append(setTimings, int(time.Since(t0)/time.Millisecond))

						if err != nil {
							setErrors++
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

						t0 := time.Now()
						err := client.Set(key, v[:], 0)
						setTimings = append(setTimings, int(time.Since(t0)/time.Millisecond))

						if err != nil {
							setErrors++
							log.Println("error during set: ", err)
						}

					}

				} else {

					if *verbose {
						log.Println("GET client=", hosts[clientNumber], "key=", key)
					}

					t0 := time.Now()
					_, err := client.Get(key)
					getTimings = append(getTimings, int(time.Since(t0)/time.Millisecond))

					if err != nil {
						getErrors++
						log.Println("error during get: ", err)
					}
				}

				select {
				case <-done:
					// the channel has been closed -- this is the shutdown signal
					resultsCh <- resultTimings{sets: setTimings, gets: getTimings, setErrs: setErrors, getErrs: getErrors}
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

	// and read back the responses
	for i := 0; i < *clients; i++ {

		r := <-resultsCh

		for _, t := range r.gets {
			fmt.Fprintln(timingsFile, "get:", t)
		}

		for _, t := range r.sets {
			fmt.Fprintln(timingsFile, "set:", t)
		}

		if r.setErrs != 0 {
			fmt.Fprintln(timingsFile, "serr: ", r.setErrs)
		}

		if r.getErrs != 0 {
			fmt.Fprintln(timingsFile, "gerr: ", r.getErrs)
		}

	}

}
