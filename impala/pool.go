package impala

import (
	"fmt"

	"github.com/jolestar/go-commons-pool"
)

type PoolConfig struct {
	Host                          string
	Port                          int
	MaxTotal                      int
	MaxIdle                       int
	MinIdle                       int
	MaxWaitMillis                 int64
	MinEvictableIdleTimeMillis    int64
	TimeBetweenEvictionRunsMillis int64
}

type Pool struct {
	cfg PoolConfig
}

func (p *Pool) MakeObject() (*pool.PooledObject, error) {
	conn, err := NewConn(p.cfg.Host, p.cfg.Port)
	return pool.NewPooledObject(conn), err
}

func (p *Pool) DestroyObject(obj *pool.PooledObject) error {
	conn, ok := obj.Object.(*Conn)
	if !ok {
		return fmt.Errorf("assert conn error, ojb: %+v", obj.Object)
	}
	return conn.Close()
}

func (p *Pool) ValidateObject(object *pool.PooledObject) bool {
	return true
}

func (p *Pool) ActivateObject(object *pool.PooledObject) error {
	return nil
}

func (p *Pool) PassivateObject(object *pool.PooledObject) error {
	return nil
}

func InitPool(cfg PoolConfig) *pool.ObjectPool {
	p := pool.NewObjectPoolWithDefaultConfig(&Pool{cfg})
	p.Config.MaxTotal = cfg.MaxTotal
	p.Config.MaxIdle = cfg.MaxIdle
	p.Config.MaxWaitMillis = cfg.MaxWaitMillis
	p.Config.MinEvictableIdleTimeMillis = cfg.MinEvictableIdleTimeMillis
	p.Config.TimeBetweenEvictionRunsMillis = cfg.TimeBetweenEvictionRunsMillis
	p.StartEvictor()
	return p
}
