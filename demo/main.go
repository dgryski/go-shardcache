package main

import (
	"flag"
	"io/ioutil"
	"log"
	"os"

	"github.com/dgryski/go-shardcache"
)

func main() {

	host := flag.String("h", "localhost:8080", "shardcache host")
	secret := flag.String("auth", "", "shardcache auth secret")
	flag.Parse()

	var auth []byte

	if *secret != "" {
		auth = make([]byte, 16)
		copy(auth[:], *secret)
	}

	client := shardcache.New(*host, auth)

	cmd := flag.Arg(0)
	switch cmd {
	case "get":
		arg := flag.Arg(1)
		r, err := client.Get([]byte(arg))
		if err != nil {
			log.Fatal("error fetching: ", err)
		}
		os.Stdout.Write(r)

	case "del":
		arg := flag.Arg(1)
		err := client.Del([]byte(arg), false)
		if err != nil {
			log.Fatal("error deleting: ", err)
		}

	case "evict":
		arg := flag.Arg(1)
		err := client.Del([]byte(arg), true)
		if err != nil {
			log.Fatal("error evicting: ", err)
		}

	case "set":
		arg := flag.Arg(1)
		fname := flag.Arg(2)
		f, err := ioutil.ReadFile(fname)
		if err != nil {
			log.Fatal("error reading", fname, ":", err)
		}
		client.Set([]byte(arg), f, 0)

	case "stats":
		r, err := client.Stats()
		if err != nil {
			log.Fatal("error getting stats: ", err)
		}
		os.Stdout.Write(r)

	case "index":
		idx, err := client.Index()
		if err != nil {
			log.Fatal("error getting index: ", err)
		}
		for _, entry := range idx {
			log.Printf("%s %d\n", entry.Key, entry.ValueSize)
		}

	default:
		log.Fatal("unknown command: ", cmd)
	}
}
