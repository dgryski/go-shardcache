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

	MSG_OK     = 0x00
	MSG_ERR    = 0xff
	MSG_YES    = 0x01
	MSG_NO     = 0xfe
	MSG_EXISTS = 0x02

	PROTOCOL_VERSION = 0x01
)

var MAGIC = []byte{0x73, 0x68, 0x63, PROTOCOL_VERSION}

type Client struct {
	conn net.Conn
}

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

	_, err := w.Write(MAGIC)
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
			_, err := w.Write([]byte{MSG_RSEP})
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

	_, err = w.Write([]byte{MSG_EOM})

	return err
}

func (c *Client) readResponse(msg byte, records int) ([][]byte, error) {

	var l [8]byte

	r := c.conn

	n, err := r.Read(l[:4])
	if err != nil {
		return nil, err
	}
	if n != 4 {
		return nil, errors.New("short read for magic")
	}

	if !bytes.Equal(l[:3], MAGIC[:3]) {
		return nil, errors.New("bad magic")
	}

	n, err = r.Read(l[:1])
	if err != nil {
		return nil, err
	}
	if n != 1 {
		return nil, errors.New("short read for response byte")
	}

	if l[0] != msg {
		return nil, errors.New("bad response byte")
	}

	var response [][]byte

	for {

		record, err := readRecord(r)
		if err != nil {
			return nil, fmt.Errorf("readRecord: %s", err)
		}

		response = append(response, record)

		n, err = r.Read(l[:1])

		if n != 1 {
			return nil, errors.New("short read waiting for next record")
		}

		if l[0] == MSG_EOM {
			// all done
			break
		}

		if l[0] != MSG_RSEP {
			return nil, errors.New("unknown byte while looking for rsep")
		}
	}

	if len(response) != records {
		return nil, errors.New("bad number of records")
	}

	return response, nil
}

func (c *Client) Get(key []byte) ([]byte, error) {

	err := c.send(MSG_GET, key)

	if err != nil {
		return nil, err
	}

	response, err := c.readResponse(MSG_RES, 1)

	if err != nil {
		return nil, err
	}

	return response[0], err
}

func (c *Client) GetOffset(key []byte, offset, length uint32) ([]byte, error) {

	var offs [4]byte
	var l [4]byte

	binary.BigEndian.PutUint32(offs[:], offset)
	binary.BigEndian.PutUint32(l[:], length)

	err := c.send(MSG_GET_OFFSET, key, offs[:], l[:])

	if err != nil {
		return nil, err
	}

	response, err := c.readResponse(MSG_RES, 2)

	if err != nil {
		return nil, err
	}

	return response[0], err
}

func (c *Client) Touch(key []byte) ([]byte, error) {

	err := c.send(MSG_TOUCH, key)

	if err != nil {
		return nil, err
	}

	response, err := c.readResponse(MSG_RES, 1)
	if err != nil {
		return nil, err
	}

	return response[0], nil
}

func (c *Client) Set(key, value []byte, expire uint32) error {
	resp, err := c.set(key, value, expire, MSG_SET)

	if err != nil {
		return err
	}

	if len(resp) != 1 {
		return errors.New("bad read for set response")

	}

	switch resp[0] {
	case MSG_OK:
		return nil
	case MSG_ERR:
		return errors.New("error during set")
	}

	return errors.New("unknown set response")
}

func (c *Client) Add(key, value []byte, expire uint32) (existed bool, err error) {
	resp, err := c.set(key, value, expire, MSG_ADD)

	if err != nil {
		return false, err
	}

	if len(resp) != 1 {
		return false, errors.New("bad read for set response")
	}

	switch resp[0] {
	case MSG_OK:
		return false, nil
	case MSG_ERR:
		return false, errors.New("error during add")
	case MSG_EXISTS:
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

	response, err := c.readResponse(MSG_RES, 1)

	if err != nil {
		return nil, err
	}

	return response[0], err
}

func (c *Client) Del(key []byte) error {
	err := c.send(MSG_DEL, key)
	if err != nil {
		return err
	}

	response, err := c.readResponse(MSG_RES, 1)
	if err != nil {
		return err
	}

	if len(response[0]) != 1 {
		return errors.New("bad delete response")
	}

	if response[0][0] != MSG_OK || response[0][0] == MSG_ERR {
		return errors.New("error during delete")
	}

	return nil
}

func (c *Client) Evict(key []byte) error {
	err := c.send(MSG_EVI, key)
	if err != nil {
		return err
	}

	response, err := c.readResponse(MSG_RES, 1)
	if err != nil {
		return err
	}

	if len(response[0]) != 1 {
		return errors.New("bad delete response")
	}

	if response[0][0] != MSG_OK || response[0][0] == MSG_ERR {
		return errors.New("error during delete")
	}

	return nil
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

	response, err := c.readResponse(MSG_IDR, 1)
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

func (c *Client) Stats() ([]byte, error) {

	err := c.send(MSG_STS, nil)
	if err != nil {
		return nil, err
	}

	response, err := c.readResponse(MSG_RES, 1)
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
