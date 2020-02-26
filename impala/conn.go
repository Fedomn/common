package impala

import (
	"context"
	"fmt"

	"github.com/fedomn/common/impala/gen-go/beeswax"
	"github.com/fedomn/common/impala/gen-go/impalaservice"

	"git.apache.org/thrift.git/lib/go/thrift"
)

type Conn struct {
	client    *impalaservice.ImpalaServiceClient
	transport thrift.TTransport
}

func NewConn(host string, port int) (*Conn, error) {
	socket, err := thrift.NewTSocket(fmt.Sprintf("%s:%d", host, port))
	if err != nil {
		return nil, err
	}

	// bufferSize = The size of the read and write buffers to use, in bytes.
	// 24M
	bufferSize := 1024 * 1024 * 24
	transportFactory := thrift.NewTBufferedTransportFactory(bufferSize)
	protocolFactory := thrift.NewTBinaryProtocolFactoryDefault()

	transport, _ := transportFactory.GetTransport(socket)

	if err := transport.Open(); err != nil {
		return nil, err
	}

	client := impalaservice.NewImpalaServiceClientFactory(transport, protocolFactory)
	return &Conn{client, transport}, nil
}

func (c *Conn) IsOpen() bool {
	return c.transport.IsOpen()
}

func (c *Conn) Close() error {
	if c.IsOpen() {
		err := c.transport.Close()
		if err != nil {
			return err
		}
		c.client = nil
		c.transport = nil
	}
	return nil
}

func (c *Conn) CreateQuery(query string) (*Rows, error) {
	bQuery := beeswax.Query{
		Query: query,
	}

	handle, err := c.client.Query(context.Background(), &bQuery)
	if err != nil {
		return nil, err
	}
	return newRows(c.client, handle), nil
}

func (c *Conn) CreateQueryWithCfg(query string, cfg []string) (*Rows, error) {
	bQuery := beeswax.Query{
		Query:         query,
		Configuration: cfg,
	}

	handle, err := c.client.Query(context.Background(), &bQuery)
	if err != nil {
		return nil, err
	}
	return newRows(c.client, handle), nil
}
