package store

import (
	"github.com/garyburd/redigo/redis"
	"github.com/vmihailenco/msgpack"
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

////////////////////////////////////////////////////////////////////////////////////////////////
// Sets key and value in a hash
////////////////////////////////////////////////////////////////////////////////////////////////
func (s *Store) HashSet(hash, key string, value interface{}) error {
	conn := s.Pool.Get()
	defer conn.Close()

	if err := conn.Err(); err != nil {
		return err
	}

	b, err := msgpack.Marshal(value)
	if err != nil {
		return err
	}

	_, err = conn.Do("HSET", hash, key, b)
	if err != nil {
		return err
	}

	return nil
}

////////////////////////////////////////////////////////////////////////////////////////////////
// Set key to hold the string value. If key already holds a value, it is overwritten, regardless of
// its type. Any previous time to live associated with the key is discarded on successful SET operation.
////////////////////////////////////////////////////////////////////////////////////////////////
func (s *Store) Set(key string, value interface{}) error {
	conn := s.Pool.Get()
	defer conn.Close()

	if err := conn.Err(); err != nil {
		return err
	}

	b, err := msgpack.Marshal(value)
	if err != nil {
		return err
	}

	if _, err := conn.Do("SET", key, b); err != nil {
		return err
	}

	return nil
}

////////////////////////////////////////////////////////////////////////////////////////////////
// Store a value with ttl.
////////////////////////////////////////////////////////////////////////////////////////////////
func (s *Store) SetWithTTL(key string, value interface{}, ttl int) error {
	conn := s.Pool.Get()
	defer conn.Close()

	if err := conn.Err(); err != nil {
		return err
	}

	b, err := msgpack.Marshal(value)
	if err != nil {
		return err
	}

	if _, err = conn.Do("SETEX", key, ttl, b); err != nil {
		return err
	}

	return nil
}

////////////////////////////////////////////////////////////////////////////////////////////////
// Get a value from a hash. A missing value will return nil, nil.
////////////////////////////////////////////////////////////////////////////////////////////////
func (s *Store) HashGet(hash, key string) (interface{}, error) {
	conn := s.Pool.Get()
	defer conn.Close()

	if err := conn.Err(); err != nil {
		return nil, err
	}

	data, err := conn.Do("HGET", hash, key)
	if err != nil {
		return nil, err
	}

	if data == nil {
		return nil, nil
	}

	b, err := redis.Bytes(data, err)
	if err != nil {
		return nil, err
	}

	var out interface{}
	err = msgpack.Unmarshal(b, &out)

	if err != nil {
		return nil, err
	}

	return out, nil
}

////////////////////////////////////////////////////////////////////////////////////////////////
// Get all keys from a hash.
////////////////////////////////////////////////////////////////////////////////////////////////
func (s *Store) HashGetKeys(hash string) ([]string, error) {
	conn := s.Pool.Get()
	defer conn.Close()

	if err := conn.Err(); err != nil {
		return nil, err
	}

	data, err := conn.Do("HKEYS", hash)
	if err != nil {
		return nil, err
	}

	if data == nil {
		return nil, nil
	}

	res, err := redis.Strings(data, err)
	if err != nil {
		return nil, err
	}

	return res, nil
}

////////////////////////////////////////////////////////////////////////////////////////////////
// Get all values from a hash
////////////////////////////////////////////////////////////////////////////////////////////////
func (s *Store) HashGetValues(hash string) ([]interface{}, error) {
	conn := s.Pool.Get()
	defer conn.Close()

	if err := conn.Err(); err != nil {
		return nil, err
	}

	data, err := conn.Do("HVALS", hash)
	if err != nil {
		return nil, err
	}

	if data == nil {
		return nil, nil
	}

	vals, err := redis.Values(data, err)
	if err != nil {
		return nil, err
	}

	out := make([]interface{}, len(vals))

	for n, val := range vals {
		if err = msgpack.Unmarshal(val.([]byte), &out[n]); err != nil {
			return nil, err
		}
	}

	return out, nil
}

////////////////////////////////////////////////////////////////////////////////////////////////
// Returns the number of fields contained in the hash stored at hash.
// Returns number of fields in the hash, or 0 when key does not exist.
////////////////////////////////////////////////////////////////////////////////////////////////
func (s *Store) HashSize(hash string) (int, error) {
	conn := s.Pool.Get()
	defer conn.Close()

	if err := conn.Err(); err != nil {
		return 0, err
	}

	data, err := conn.Do("HLEN", hash)
	if err != nil {
		return 0, err
	}

	if data == nil {
		return 0, nil
	}

	res, err := redis.Int(data, err)
	if err != nil {
		return 0, err
	}

	return res, nil
}

////////////////////////////////////////////////////////////////////////////////////////////////
// Removes the specified fields from the hash stored at field. Specified fields that do not exist within this
// hash are ignored. If key does not exist, it is treated as an empty hash and this command returns 0.
// Returns the number of fields that were removed from the hash, not including specified but non existing fields.
////////////////////////////////////////////////////////////////////////////////////////////////
func (s *Store) HashDeleteField(hash, field string) (int, error) {
	conn := s.Pool.Get()
	defer conn.Close()

	if err := conn.Err(); err != nil {
		return 0, err
	}

	data, err := conn.Do("HDEL", hash, field)
	if err != nil {
		return 0, err
	}

	if data == nil {
		return 0, nil
	}

	res, err := redis.Int(data, err)
	if err != nil {
		return 0, err
	}

	return res, nil
}

////////////////////////////////////////////////////////////////////////////////////////////////
// Get a stored value. A missing value will return nil, nil.
////////////////////////////////////////////////////////////////////////////////////////////////
func (s *Store) Get(key string) (interface{}, error) {
	conn := s.Pool.Get()
	defer conn.Close()

	if err := conn.Err(); err != nil {
		return nil, err
	}

	data, err := conn.Do("GET", key)
	if err != nil {
		return nil, err
	}

	if data == nil {
		return nil, nil
	}

	b, err := redis.Bytes(data, err)
	if err != nil {
		return nil, err
	}

	var out interface{}
	if err = msgpack.Unmarshal(b, &out); err != nil {
		return nil, err
	}

	return out, nil
}

////////////////////////////////////////////////////////////////////////////////////////////////
// Delete a value
////////////////////////////////////////////////////////////////////////////////////////////////
func (s *Store) Delete(key string) error {
	conn := s.Pool.Get()
	defer conn.Close()

	if _, err := conn.Do("DEL", key); err != nil {
		return err
	}

	return nil
}

////////////////////////////////////////////////////////////////////////////////////////////////
// Pushes a value to a list
////////////////////////////////////////////////////////////////////////////////////////////////
func (s *Store) ListPush(list, key string, value interface{}) error {
	conn := s.Pool.Get()
	defer conn.Close()

	if err := conn.Err(); err != nil {
		return err
	}

	b, err := msgpack.Marshal(value)

	if err != nil {
		return err
	}

	_, err = conn.Do("LPUSH", list, key, b)

	if err != nil {
		return err
	}

	return nil
}
