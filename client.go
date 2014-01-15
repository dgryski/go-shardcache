package shardcache

import (
	"bytes"
	"crypto/hmac"
	"encoding/binary"
	"errors"
	"fmt"
	"hash"
	"io"
	"math"
	"net"

	"github.com/dchest/siphash"
)

const (
	MSG_GET        byte = 0x01
	MSG_SET             = 0x02
	MSG_DEL             = 0x03
	MSG_EVI             = 0x04
	MSG_GET_ASYNC       = 0x05
	MSG_GET_OFFSET      = 0x06
	MSG_ADD             = 0x07
	MSG_TOUCH           = 0x08

	MSG_CHK = 0x31
	MSG_STS = 0x32

	MSG_IDG = 0x41
	MSG_IDR = 0x42

	MSG_EOM  = 0x00
	MSG_RSEP = 0x80
	MSG_RES  = 0x99

	SIG_HDR = 0xF0

	PROTOCOL_VERSION = 0x01
)

var MAGIC []byte = []byte{0x73, 0x68, 0x63, PROTOCOL_VERSION}

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

func (c *Client) send(msg byte, args ...[]byte) error {

	var w io.Writer
	var sig hash.Hash

	c.conn.Write(MAGIC)

	if c.auth != nil {
		sig = siphash.New(c.auth)
		c.conn.Write([]byte{SIG_HDR})
		w = io.MultiWriter(c.conn, sig)
	} else {
		w = c.conn
	}

	_, err := w.Write([]byte{msg})
	if err != nil {
		return err
	}

	needSep := false

	for _, a := range args {
		if needSep {
			w.Write([]byte{MSG_RSEP})
		}
		writeRecord(w, a)
		needSep = true
	}

	_, err = w.Write([]byte{MSG_EOM})

	if err != nil {
		return err
	}

	if c.auth != nil {
		_, err = c.conn.Write(sig.Sum(nil))
	}

	return err
}

func (c *Client) readResponse(msg byte) ([]byte, error) {

	var sig hash.Hash

	var r io.Reader

	var m [4]byte
	n, err := c.conn.Read(m[:4])
	if n != 4 || err != nil {
		return nil, err
	}

	if !bytes.Equal(m[0:3], MAGIC[0:3]) {
		return nil, errors.New("Magic doesn't match")
	}

	var l [8]byte
	n, err = c.conn.Read(l[:1])
	if n != 1 || err != nil {
		return nil, err
	}

	if c.auth != nil {
		if l[0] != SIG_HDR {
			return nil, errors.New("SIG_HDR not found")
		}

		sig = siphash.New(c.auth)
		r = io.TeeReader(c.conn, sig)

		n, err := r.Read(l[:1])
		if n != 1 || err != nil {
			return nil, err
		}
	} else {
		if l[0] == SIG_HDR {
			return nil, errors.New("SIG_HDR not expected")
		}
		r = c.conn
	}

	if l[0] != msg {
		return nil, errors.New("bad response byte")
	}

	response, err := readRecord(r)
	if err != nil {
		return nil, fmt.Errorf("readRecord: %s", err)
	}

	r.Read(l[:1])
	if l[0] != MSG_EOM {
		return nil, errors.New("bad EOM")
	}

	if c.auth != nil {
		// read signature
		c.conn.Read(l[:])
		sum := sig.Sum(nil)

		if !hmac.Equal(sum, l[:]) {
			return nil, errors.New("bad signature")
		}
	}

	return response, nil
}

func (c *Client) Get(key []byte) ([]byte, error) {

	err := c.send(MSG_GET, key)

	if err != nil {
		return nil, err
	}

	response, err := c.readResponse(MSG_RES)

	return response, err
}

func (c *Client) GetOffset(key []byte, offset, length uint32) ([]byte, error) {

	var b [8]byte
	binary.BigEndian.PutUint32(b[:], offset)
	binary.BigEndian.PutUint32(b[4:], length)

	msg := append(key, b[:]...)

	err := c.send(MSG_GET_OFFSET, msg)

	if err != nil {
		return nil, err
	}

	response, err := c.readResponse(MSG_RES)

	return response, err
}

func (c *Client) Touch(key []byte) ([]byte, error) {

	err := c.send(MSG_TOUCH, key)

	if err != nil {
		return nil, err
	}

	response, err := c.readResponse(MSG_RES)

	return response, err
}

func (c *Client) Set(key, value []byte, expire uint32) error {
	resp, err := c.set(key, value, expire, MSG_SET)

	if len(resp) != 2 || resp[0] != 'O' || resp[1] != 'K' {
		return errors.New("bad set response")
	}

	return err
}

func (c *Client) Add(key, value []byte, expire uint32) (existed bool, err error) {
	resp, err := c.set(key, value, expire, MSG_ADD)

	switch string(resp) {
	case "YES":
		return true, nil
	case "NO":
		return false, nil
	case "ERR":
		return false, errors.New("error during add")
	}
	return false, errors.New("unknown add response")
}

func (c *Client) set(key, value []byte, expire uint32, msgbyte byte) ([]byte, error) {

	var err error

	if expire == 0 {
		err = c.send(msgbyte, key, value)
	} else {
		var expBytes [4]byte
		binary.BigEndian.PutUint32(expBytes[:], expire)
		err = c.send(msgbyte, key, value, expBytes[:])
	}

	response, err := c.readResponse(MSG_RES)

	return response, err

}

func (c *Client) Del(key []byte) error {
	return c.send(MSG_DEL, key)
}

func (c *Client) Evict(key []byte) error {
	return c.send(MSG_EVI, key)
}

type DirEntry struct {
	Key       []byte
	ValueSize int
}

func (c *Client) Index() ([]DirEntry, error) {

	err := c.send(MSG_IDG, nil)
	if err != nil {
		return nil, err
	}

	idxBuf, err := c.readResponse(MSG_IDR)
	if err != nil {
		return nil, err
	}

	var index []DirEntry

	// extract data from index buffer
	for len(idxBuf) > 0 {
		klen := binary.BigEndian.Uint32(idxBuf)
		if klen == 0 {
			break
		}
		idxBuf = idxBuf[4:]
		key := make([]byte, klen)
		copy(key, idxBuf)
		idxBuf = idxBuf[klen:]
		vlen := binary.BigEndian.Uint32(idxBuf)
		idxBuf = idxBuf[4:]
		index = append(index, DirEntry{Key: key, ValueSize: int(vlen)})
	}

	return index, nil
}

func (c *Client) Stats() ([]byte, error) {

	c.send(MSG_STS, nil)

	response, err := c.readResponse(MSG_RES)

	return response, err
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
