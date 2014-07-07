package main

import (
	"flag"
	"io/ioutil"
	"log"
	"os"
	"strconv"

	"github.com/dgryski/go-shardcache"
)

func main() {

	host := flag.String("h", "localhost:4444", "shardcache host")
	flag.Parse()

	client, err := shardcache.New(*host, nil)

	if err != nil {
		log.Fatal("unable to create client:", err)
	}

	cmd := flag.Arg(0)
	switch cmd {
	case "get":
		arg := flag.Arg(1)
		r, err := client.Get([]byte(arg), nil)
		if err != nil {
			log.Fatal("error fetching: ", err)
		}
		os.Stdout.Write(r)

	case "getoffs":
		arg := flag.Arg(1)
		offs, _ := strconv.Atoi(flag.Arg(2))
		length, _ := strconv.Atoi(flag.Arg(3))
		r, _, err := client.GetOffset([]byte(arg), uint32(offs), uint32(length))
		if err != nil {
			log.Fatal("error fetching: ", err)
		}
		os.Stdout.Write(r)

	case "getasync":
		arg := flag.Arg(1)
		r, err := client.GetAsync([]byte(arg), nil)
		if err != nil {
			log.Fatal("error fetching: ", err)
		}
		os.Stdout.Write(r)

	case "del":
		arg := flag.Arg(1)
		err := client.Delete([]byte(arg))
		if err != nil {
			log.Fatal("error deleting: ", err)
		}

	case "evict":
		arg := flag.Arg(1)
		err := client.Evict([]byte(arg))
		if err != nil {
			log.Fatal("error evicting: ", err)
		}

	case "set", "add":
		key := flag.Arg(1)
		arg := flag.Arg(2)
		var val []byte
		if arg[0] == '@' {
			var err error
			val, err = ioutil.ReadFile(arg[1:])
			if err != nil {
				log.Fatal("error reading", arg[1:], ":", err)
			}
		} else {
			val = []byte(arg)
		}
		var err error
		if cmd == "set" {
			err = client.Set([]byte(key), val, 0)
		} else {
			// cmd == "add"
			var existed bool
			existed, err = client.Add([]byte(key), val, 0)
			if existed {
				log.Println("key already exists")
			}
		}
		if err != nil {
			log.Fatal("error setting key: ", err)
		}

	case "touch":
		arg := flag.Arg(1)
		r, err := client.Touch([]byte(arg), nil)
		if err != nil {
			log.Fatal("error touching: ", err)
		}
		os.Stdout.Write(r)

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
