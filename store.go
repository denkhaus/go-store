package store

import (
	"github.com/garyburd/redigo/redis"
	"time"
)

// Provides a redis backed Store.
type Store struct {
	Pool *redis.Pool
}

////////////////////////////////////////////////////////////////////////////////////////////////
// ping does an internal ping against a server to check if it is alive.
////////////////////////////////////////////////////////////////////////////////////////////////
func (s *Store) ping() (bool, error) {
	conn := s.Pool.Get()
	defer conn.Close()

	data, err := conn.Do("PING")

	if err != nil || data == nil {
		return false, err
	}

	return (data == "PONG"), nil
}

////////////////////////////////////////////////////////////////////////////////////////////////
//
////////////////////////////////////////////////////////////////////////////////////////////////
func dial(network, address, password string) (redis.Conn, error) {
	c, err := redis.Dial(network, address)

	if err != nil {
		return nil, err
	}

	if password != "" {
		if _, err := c.Do("AUTH", password); err != nil {
			c.Close()
			return nil, err
		}
	}

	return c, err
}

////////////////////////////////////////////////////////////////////////////////////////////////
// NewStore returns a new RediStore.
// size: maximum number of idle connections.
////////////////////////////////////////////////////////////////////////////////////////////////

func NewStore(size int, network, address, password string) (*Store, error) {
	return NewStoreWithPool(&redis.Pool{
		MaxIdle:     size,
		IdleTimeout: 240 * time.Second,
		TestOnBorrow: func(c redis.Conn, t time.Time) error {
			_, err := c.Do("PING")
			return err
		},
		Dial: func() (redis.Conn, error) {
			return dial(network, address, password)
		},
	})
}

////////////////////////////////////////////////////////////////////////////////////////////////
// NewStoreWithDB - like NewRedisStore but accepts `DB` parameter to select
// redis DB instead of using the default one ("0")
////////////////////////////////////////////////////////////////////////////////////////////////
func NewStoreWithDB(size int, network, address, password, DB string) (*Store, error) {
	rs, _ := NewStore(size, network, address, password)
	rs.Pool.Dial = func() (redis.Conn, error) {
		c, err := dial(network, address, password)

		if err != nil {
			return c, err
		}

		if _, err := c.Do("SELECT", DB); err != nil {
			c.Close()
			return nil, err
		}

		return c, err
	}

	_, err := rs.ping()
	return rs, err
}

////////////////////////////////////////////////////////////////////////////////////////////////
// NewStoreWithPool instantiates a RediStore with a *redis.Pool passed in.
////////////////////////////////////////////////////////////////////////////////////////////////
func NewStoreWithPool(pool *redis.Pool) (*Store, error) {
	rs := &Store{Pool: pool}
	_, err := rs.ping()
	return rs, err
}

////////////////////////////////////////////////////////////////////////////////////////////////
// Close closes the underlying *redis.Pool
////////////////////////////////////////////////////////////////////////////////////////////////
func (s *Store) Close() error {
	return s.Pool.Close()
}
