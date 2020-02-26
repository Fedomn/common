package impala_test

import (
	"fmt"
	"testing"
	"time"

	. "github.com/fedomn/common/impala"
	"github.com/jolestar/go-commons-pool"
)

var p *pool.ObjectPool

func TestMain(m *testing.M) {
	cfg := PoolConfig{
		Host:                          "127.0.0.1",
		Port:                          21000,
		MaxTotal:                      2,
		MaxIdle:                       1,
		MinIdle:                       0,
		MaxWaitMillis:                 1000 * 2,
		MinEvictableIdleTimeMillis:    1000 * 3,
		TimeBetweenEvictionRunsMillis: 1000,
	}
	p = InitPool(cfg)
	m.Run()
}

func TestEvictIdle(t *testing.T) {
	fmt.Printf("Init Pool Conn Active: %d, Idel: %d\n", p.GetNumActive(), p.GetNumIdle())
	conn1, _ := p.BorrowObject()
	p.BorrowObject()
	fmt.Printf("Get conn1, conn2. Pool Conn Active: %d, Idel: %d\n", p.GetNumActive(), p.GetNumIdle())

	p.ReturnObject(conn1)
	fmt.Printf("Return conn1. Pool Conn Active: %d, Idel: %d\n", p.GetNumActive(), p.GetNumIdle())

	fmt.Println("time sleep 5s")
	time.Sleep(time.Second * 5)

	fmt.Printf("Conn Active: %d, Idel: %d\n", p.GetNumActive(), p.GetNumIdle())
}

func TestMaxWaitMillis(t *testing.T) {
	fmt.Printf("Init Pool Conn Active: %d, Idel: %d\n", p.GetNumActive(), p.GetNumIdle())
	p.BorrowObject()
	p.BorrowObject()
	conn, err := p.BorrowObject()
	fmt.Println(conn, err)
}

func TestMaxIdle(t *testing.T) {
	fmt.Printf("Init Pool Conn Active: %d, Idel: %d\n", p.GetNumActive(), p.GetNumIdle())
	conn1, _ := p.BorrowObject()
	conn2, _ := p.BorrowObject()
	p.ReturnObject(conn1)
	p.ReturnObject(conn2)

	fmt.Printf("conn1: %+v\n", conn1)
	fmt.Printf("conn2: %+v\n", conn2)
	fmt.Printf("Conn Active: %d, Idel: %d\n", p.GetNumActive(), p.GetNumIdle())
}
