package shardcache

import (
	"bytes"
	"crypto/hmac"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"net"

	"github.com/dchest/siphash"
)

const (
	MSG_GET  byte = 0x01
	MSG_SET       = 0x02
	MSG_DEL       = 0x03
	MSG_EVI       = 0x04
	MSG_EOM       = 0x00
	MSG_RSEP      = 0x80
	MSG_RES       = 0x99
)

type Client struct {
	auth []byte
	conn net.Conn
}

func New(host string, auth []byte) *Client {

	conn, _ := net.Dial("tcp", host)

	return &Client{
		auth: auth,
		conn: conn,
	}
}

func (c *Client) Get(key []byte) ([]byte, error) {

	var bufw = &bytes.Buffer{}

	sig := siphash.New(c.auth)

	w := io.MultiWriter(bufw, sig)

	w.Write([]byte{MSG_GET})
	writeRecord(w, key)
	w.Write([]byte{MSG_EOM})

	bufw.Write(sig.Sum(nil))
	c.conn.Write(bufw.Bytes())

	sig.Reset()

	r := io.TeeReader(c.conn, sig)

	var l [8]byte
	n, err := r.Read(l[:1])
	if n != 1 || err != nil {
		return nil, err
	}

	if l[0] != MSG_RES {
		return nil, errors.New("bad response byte")
	}

	record, err := readRecord(r)
	if err != nil {
		return nil, fmt.Errorf("readRecord: %s", err)

	}

	r.Read(l[:1])
	if l[0] != MSG_EOM {
		return nil, errors.New("bad EOM")
	}

	// read signature
	c.conn.Read(l[:])

	sum := sig.Sum(nil)

	if !hmac.Equal(sum, l[:]) {
		return nil, errors.New("bad signature")
	}

	return record, nil
}

func (c *Client) Set(key, value []byte, expire uint32) error {

	var bufw = &bytes.Buffer{}

	sig := siphash.New(c.auth)

	w := io.MultiWriter(bufw, sig)

	w.Write([]byte{MSG_SET})
	writeRecord(w, key)
	w.Write([]byte{MSG_RSEP})
	writeRecord(w, value)

	if expire != 0 {
		var l [4]byte
		binary.BigEndian.PutUint32(l[:], expire)
		w.Write([]byte{MSG_RSEP})
		writeRecord(w, l[:])
	}

	w.Write([]byte{MSG_EOM})

	bufw.Write(sig.Sum(nil))

	c.conn.Write(bufw.Bytes())

	return nil
}

func (c *Client) Del(key []byte, evict bool) error {

	var bufw = &bytes.Buffer{}

	sig := siphash.New(c.auth)

	w := io.MultiWriter(bufw, sig)

	var cmd byte
	if evict {
		cmd = MSG_EVI
	} else {
		cmd = MSG_DEL
	}

	w.Write([]byte{cmd})
	writeRecord(w, key)
	w.Write([]byte{MSG_EOM})

	bufw.Write(sig.Sum(nil))
	c.conn.Write(bufw.Bytes())

	return nil
}

func writeRecord(w io.Writer, record []byte) {
	l := []byte{0, 0}

	for len(record) > 0 {
		var blockSize uint16

		if len(record) > math.MaxUint16 {
			blockSize = math.MaxUint16
		} else {
			blockSize = uint16(len(record))
		}

		binary.BigEndian.PutUint16(l, blockSize)
		w.Write(l)
		w.Write(record[:blockSize])
		record = record[blockSize:]
	}

	l[0] = 0
	l[1] = 0
	w.Write(l)
}

func readRecord(r io.Reader) ([]byte, error) {
	l := []byte{0, 0}

	var record []byte

	block := make([]byte, math.MaxUint16)

	n, err := io.ReadFull(r, l)
	for n == 2 && err == nil {
		blockSize := binary.BigEndian.Uint16(l)
		if blockSize == 0 {
			break
		}
		io.ReadFull(r, block[:blockSize])
		record = append(record, block[:blockSize]...)
		n, err = io.ReadFull(r, l)
	}

	return record, err
}
