package store

import (
	"github.com/garyburd/redigo/redis"
	"github.com/vmihailenco/msgpack"
)

// Provides a redis backed Store.
type Store struct {
	Pool          *redis.Pool
	DefaultMaxAge int // default Redis TTL for a MaxAge == 0 session
	maxLength     int
}

// ping does an internal ping against a server to check if it is alive.
func (s *Store) ping() (bool, error) {
	conn := s.Pool.Get()
	defer conn.Close()

	data, err := conn.Do("PING")

	if err != nil || data == nil {
		return false, err
	}

	return (data == "PONG"), nil
}

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

// NewRediStore returns a new RediStore.
// size: maximum number of idle connections.
func NewStore(size int, network, address, password string, keyPairs ...[]byte) (*Store, error) {
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
	}, keyPairs...)
}

// NewRedisStoreWithDB - like NewRedisStore but accepts `DB` parameter to select
// redis DB instead of using the default one ("0")
func NewStoreWithDB(size int, network, address, password, DB string, keyPairs ...[]byte) (*Store, error) {
	rs, _ := NewStore(size, network, address, password, keyPairs...)
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

// NewRediStoreWithPool instantiates a RediStore with a *redis.Pool passed in.
func NewStoreWithPool(pool *redis.Pool, keyPairs ...[]byte) (*Store, error) {
	rs := &Store{
		// http://godoc.org/github.com/garyburd/redigo/redis#Pool
		Pool:          pool,
		DefaultMaxAge: 60 * 20, // 20 minutes seems like a reasonable default
		maxLength:     4096,
	}

	_, err := rs.ping()
	return rs, err
}

// Close closes the underlying *redis.Pool
func (s *Store) Close() error {
	return s.Pool.Close()
}

// Store a value.
func (c *Store) Set(key string, value interface{}) (bool, error) {
	conn := s.Pool.Get()
	defer conn.Close()

	if err := conn.Err(); err != nil {
		return false, err
	}
}

// Get a stored value. A missing value will return nil, nil.
func (c *Store) Get(key string, out interface{}) (bool, error) {
	conn := s.Pool.Get()
	defer conn.Close()

	if err := conn.Err(); err != nil {
		return false, err
	}
	data, err := conn.Do("GET", key)
	if err != nil {
		return false, err
	}
	if data == nil {
		return false, nil // no data was associated with this key
	}
	b, err := redis.Bytes(data, err)
	if err != nil {
		return false, err
	}

	err =: msgpack.Unmarshal(b, &out)

	if err != nil {
		return false, err
	}

	return true, nil
}

func (s *Store) Delete(key string) error {
	conn := s.Pool.Get()
	defer conn.Close()

	if _, err := conn.Do("DEL", key); err != nil {
		return err
	}

	return nil
}
