package main

import (
	"encoding/binary"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"net"
	"os"
	"os/signal"
	"runtime/pprof"
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
	clients := flag.Int("clients", 0, "number of concurrent clients")
	write := flag.Int("w", 50, "percentage of calls which should be writes")
	keystr := flag.String("k", "stress", "basename key to write to")
	nkey := flag.Int("nk", 10, "number of keys to set/get")
	verbose := flag.Bool("v", false, "verbose logging")
	deletekeys := flag.Bool("del", false, "delete keys instead of writing")
	getIndex := flag.Bool("idx", false, "query shardcache for keys to use instead of generating random keys")
	timeout := flag.Duration("timeout", 0, "length of time to run")
	rate := flag.Int("rate", 0, "rate (qps)")
	timings := flag.Bool("timings", true, "log response timing metrics")
	dialTimeout := flag.Duration("dial-timeout", 5*time.Second, "dialer timeout")
	memstats := flag.String("memstats", "", "log memory stats at the end")

	flag.Parse()

	if *clients == 0 && *rate == 0 {
		log.Fatal("must provide one of: -clients / -rate")
	}

	hosts := strings.Split(*host, ",")
	nhosts := len(hosts)

	var keys [][]byte

	client, err := shardcache.New(hosts[0], &net.Dialer{Timeout: *dialTimeout})

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

	if *clients > 0 {

		for i := 0; i < *clients; i++ {

			go func(done <-chan struct{}, resultsCh chan<- Results) {
				var clients []*shardcache.Client
				for _, h := range hosts {
					client, err := shardcache.New(h, &net.Dialer{Timeout: *dialTimeout})

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

					doWrite := rnd.Intn(100) < *write

					run(&r, client, doWrite, *deletekeys, key, val)

					if *timings {
						results = append(results, r)
					}

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

	} else if *rate > 0 {

		ticks := time.Tick(time.Second / time.Duration(*rate))

		localResults := make(chan Result)
		rnd := rand.New(rand.NewSource(time.Now().UnixNano() + rand.Int63()))
		go func(done <-chan struct{}, resultsCh chan<- Results) {

			for {

				select {
				case <-ticks:

				case <-done:
					return
				}

				irnd := rand.New(rand.NewSource(rnd.Int63()))
				go func() {
					key := keys[irnd.Intn(*nkey)]
					host := hosts[irnd.Intn(nhosts)]

					doWrite := rnd.Intn(100) < *write

					t0 := time.Now()
					r := Result{
						Start: t0.UnixNano(),
					}

					client, err := shardcache.New(host, &net.Dialer{Timeout: *dialTimeout})

					if err != nil {
						log.Println("error during connect: ", err)
						r.Err = err
						r.Duration = time.Since(t0)
						localResults <- r
						return
					}

					finished := make(chan struct{})
					go func(finished chan<- struct{}) {
						run(&r, client, doWrite, *deletekeys, key, val)
						finished <- struct{}{}
					}(finished)

					select {
					case <-finished:
						// yay
					case <-done:
						// asked to shutdown
						log.Println("Interrupting client ", host, "blocked on: ", key)
						client.Close()
						return
					}

					localResults <- r

				}()
			}

		}(done, resultsCh)

		go func() {

			results := make(Results, 0, 1000)
			var timeout <-chan time.Time
			for {
				// something to collect the results
				select {
				case r := <-localResults:
					if *timings {
						results = append(results, r)
					}
				case <-done:
					// wait 2 seconds after the done signal for any stragglers
					done = nil // stop watching
					timeout = time.After(2 * time.Second)

				case <-timeout:
					// any outstanding requests when we get the shutdown signal are lost -- we don't wait for them
					resultsCh <- results
					return
				}
			}
		}()

	} else {

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

	var jenc *json.Encoder
	if *timings {
		fname := fmt.Sprintf("loadtest-%s.out", time.Now().Format("20060102150405"))
		timingsFile, _ := os.Create(fname)
		defer timingsFile.Close()
		log.Println("writing load-test results to", fname)
		jenc = json.NewEncoder(timingsFile)
	}

	// send quit to all children
	close(done)

	var results Results

	// and read back the responses so the children all clean up

	if *rate > 0 {
		*clients = 1
	}

	for i := 0; i < *clients; i++ {
		results = append(results, <-resultsCh...)
	}

	if *memstats != "" {
		f, _ := os.Create(*memstats)
		defer f.Close()
		pprof.WriteHeapProfile(f)
	}

	if *timings {
		log.Println("results from ", len(results), "requests")
		sort.Sort(results)
		jenc.Encode(results)
	}

}

func run(r *Result, client *shardcache.Client, doWrite bool, deletekeys bool, key []byte, val *uint64) {

	var dst [128]byte

	if doWrite {

		r.Write = true
		if deletekeys {

			t0 := time.Now()
			r.Start = t0.UnixNano()
			r.Err = client.Delete(key)
			r.Duration = time.Since(t0)

			if r.Err != nil {
				log.Println("error during delete: ", r.Err)
			}

		} else {
			// set
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
		t0 := time.Now()
		r.Start = t0.UnixNano()
		_, r.Err = client.Get(key, dst[:])
		r.Duration = time.Since(t0)

		if r.Err != nil {
			log.Println("error during get: ", r.Err)
		}
	}
}
