package shardcache

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
)

const (
	msgGet       byte = 0x01
	msgSet            = 0x02
	msgDel            = 0x03
	msgEvict          = 0x04
	msgGetAsync       = 0x05
	msgGetOffset      = 0x06
	msgAdd            = 0x07
	msgTouch          = 0x08

	msgCheck = 0x31
	msgStats = 0x32

	msgIndexGet      = 0x41
	msgIndexResponse = 0x42

	msgEOM             = 0x00
	msgRecordSeparator = 0x80
	msgResponse        = 0x99

	msgOK     = 0x00
	msgERR    = 0xff
	msgYES    = 0x01
	msgNO     = 0xfe
	msgExists = 0x02
)

const protocolVersion = 0x01

var protocolMagic = []byte{0x73, 0x68, 0x63, protocolVersion}

// Client is a ShardCache client
type Client struct {
	conn net.Conn
}

// New returns a ShardCache client connecting to a particular host
func New(host string) (*Client, error) {

	conn, err := net.Dial("tcp", host)

	if err != nil {
		return nil, err
	}

	return &Client{
		conn: conn,
	}, nil
}

func (c *Client) send(msg byte, args ...[]byte) error {

	w := c.conn

	_, err := w.Write(protocolMagic)
	if err != nil {
		return err
	}

	_, err = w.Write([]byte{msg})
	if err != nil {
		return err
	}
	needSep := false

	for _, a := range args {
		if needSep {
			_, err := w.Write([]byte{msgRecordSeparator})
			if err != nil {
				return err
			}
		}
		err := writeRecord(w, a)
		if err != nil {
			return err
		}
		needSep = true
	}

	_, err = w.Write([]byte{msgEOM})

	return err
}

func (c *Client) readResponse(msg byte, records int) ([][]byte, error) {

	var l [5]byte // magic + response byte

	r := c.conn

	n, err := io.ReadFull(r, l[:])
	if err != nil {
		return nil, err
	}

	if !bytes.Equal(l[:3], protocolMagic[:3]) {
		return nil, errors.New("bad magic")
	}

	// l[3], the protocol version byte, is ignored

	if l[4] != msg {
		return nil, errors.New("bad response byte")
	}

	var response [][]byte

	for {

		record, err := readRecord(r)
		if err != nil {
			return nil, fmt.Errorf("readRecord: %s", err)
		}

		response = append(response, record)

		var b [1]byte

		// we're only reading a single byte, we don't care about any error here
		n, _ = r.Read(b[:])

		if n != 1 {
			return nil, errors.New("short read waiting for next record")
		}

		if b[0] == msgEOM {
			// all done
			break
		}

		if b[0] != msgRecordSeparator {
			return nil, errors.New("unknown byte while looking for rsep")
		}
	}

	if len(response) != records {
		return nil, errors.New("bad number of records")
	}

	return response, nil
}

// Get returns the value for a particular key
func (c *Client) Get(key []byte) ([]byte, error) {

	err := c.send(msgGet, key)

	if err != nil {
		return nil, err
	}

	response, err := c.readResponse(msgResponse, 1)

	if err != nil {
		return nil, err
	}

	return response[0], err
}

// GetAsync returns the value for a particular key, but using the asynchronous interface.  The two types of Gets behave identically in the Go implementation.
func (c *Client) GetAsync(key []byte) ([]byte, error) {

	err := c.send(msgGetAsync, key)

	if err != nil {
		return nil, err
	}

	response, err := c.readResponse(msgResponse, 1)

	if err != nil {
		return nil, err
	}

	return response[0], err
}

// GetOffset returns a partial value for a key.
func (c *Client) GetOffset(key []byte, offset, length uint32) ([]byte, uint32, error) {

	var offs [4]byte
	var l [4]byte

	binary.BigEndian.PutUint32(offs[:], offset)
	binary.BigEndian.PutUint32(l[:], length)

	err := c.send(msgGetOffset, key, offs[:], l[:])

	if err != nil {
		return nil, 0, err
	}

	response, err := c.readResponse(msgResponse, 2)

	if err != nil {
		return nil, 0, err
	}

	if len(response[1]) != 4 {
		return nil, 0, errors.New("bad response size for length")
	}

	remaining := binary.BigEndian.Uint32(response[1])

	return response[0], remaining, err
}

// Touch instructs the cache to load/refresh the specified key from the storage backend.
func (c *Client) Touch(key []byte) ([]byte, error) {

	err := c.send(msgTouch, key)

	if err != nil {
		return nil, err
	}

	response, err := c.readResponse(msgResponse, 1)
	if err != nil {
		return nil, err
	}

	return response[0], nil
}

// Set sets the value of a key, with an optional expiration time (as a unix timestamp.)
func (c *Client) Set(key, value []byte, expire uint32) error {
	resp, err := c.set(key, value, expire, msgSet)

	if err != nil {
		return err
	}

	if len(resp) != 1 {
		return errors.New("bad read for set response")

	}

	switch resp[0] {
	case msgOK:
		return nil
	case msgERR:
		return errors.New("error during set")
	}

	return errors.New("unknown set response")
}

// Add sets the value of a key, with an optional expiration time.  This call returns a boolean indicating if the key already exists on the server.
func (c *Client) Add(key, value []byte, expire uint32) (existed bool, err error) {
	resp, err := c.set(key, value, expire, msgAdd)

	if err != nil {
		return false, err
	}

	if len(resp) != 1 {
		return false, errors.New("bad read for set response")
	}

	switch resp[0] {
	case msgOK:
		return false, nil
	case msgERR:
		return false, errors.New("error during add")
	case msgExists:
		return true, nil
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

	response, err := c.readResponse(msgResponse, 1)

	if err != nil {
		return nil, err
	}

	return response[0], err
}

// Del deletes the specified key from the cache
func (c *Client) Del(key []byte) error {
	err := c.send(msgDel, key)
	if err != nil {
		return err
	}

	response, err := c.readResponse(msgResponse, 1)
	if err != nil {
		return err
	}

	if len(response[0]) != 1 {
		return errors.New("bad delete response")
	}

	if response[0][0] != msgOK || response[0][0] == msgERR {
		return errors.New("error during delete")
	}

	return nil
}

// Evict deletes the specified key from the cache and also the shadow cache.
func (c *Client) Evict(key []byte) error {
	err := c.send(msgEvict, key)
	if err != nil {
		return err
	}

	response, err := c.readResponse(msgResponse, 1)
	if err != nil {
		return err
	}

	if len(response[0]) != 1 {
		return errors.New("bad delete response")
	}

	if response[0][0] != msgOK || response[0][0] == msgERR {
		return errors.New("error during delete")
	}

	return nil
}

// DirEntry is information about a single item in the cache
type DirEntry struct {
	Key       []byte
	ValueSize int
}

// Index returns the list of items currently in the cache
func (c *Client) Index() ([]DirEntry, error) {

	err := c.send(msgIndexGet, nil)
	if err != nil {
		return nil, err
	}

	response, err := c.readResponse(msgIndexResponse, 1)
	if err != nil {
		return nil, err
	}

	idxBuf := response[0]

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

// Stats returns statistics about the shardcache instance
func (c *Client) Stats() ([]byte, error) {

	err := c.send(msgStats, nil)
	if err != nil {
		return nil, err
	}

	response, err := c.readResponse(msgResponse, 1)
	if err != nil {
		return nil, err
	}

	return response[0], err
}

func writeRecord(w io.Writer, record []byte) error {
	l := []byte{0, 0}

	for len(record) > 0 {
		var blockSize uint16

		if len(record) > math.MaxUint16 {
			blockSize = math.MaxUint16
		} else {
			blockSize = uint16(len(record))
		}

		binary.BigEndian.PutUint16(l, blockSize)
		_, err := w.Write(l)
		if err != nil {
			return err
		}
		_, err = w.Write(record[:blockSize])
		if err != nil {
			return err
		}
		record = record[blockSize:]
	}

	l[0] = 0
	l[1] = 0
	_, err := w.Write(l)

	return err
}

func readRecord(r io.Reader) ([]byte, error) {
	l := []byte{0, 0}

	var record []byte

	block := make([]byte, math.MaxUint16)

	for {
		_, err := io.ReadFull(r, l)
		if err != nil {
			return nil, err
		}

		blockSize := binary.BigEndian.Uint16(l)
		if blockSize == 0 {
			break
		}

		_, err = io.ReadFull(r, block[:blockSize])
		if err != nil {
			return nil, err
		}
		record = append(record, block[:blockSize]...)
	}

	return record, nil
}
