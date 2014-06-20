package store

import (
	"github.com/garyburd/redigo/redis"
	"github.com/vmihailenco/msgpack"
)

////////////////////////////////////////////////////////////////////////////////////////////////
// Set key to hold the string value. If key already holds a value, it is overwritten, regardless of
// its type. Any previous time to live associated with the key is discarded on successful SET operation.
// Returns error if operation isn't successfull.
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

	_, err := conn.Do("SET", key, b)
	return err
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

	_, err = conn.Do("SETEX", key, ttl, b)
	return err
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

	_, err := conn.Do("DEL", key)
	return err
}

////////////////////////////////////////////////////////////////////////////////////////////////
// Enumerate
////////////////////////////////////////////////////////////////////////////////////////////////
func (s *Store) Enumerate(cursor int, match string, count int) (int, []string, error) {
	conn := s.Pool.Get()
	defer conn.Close()

	data, err := conn.Do("SCAN", cursor, "MATCH", match, "COUNT", count)
	if err != nil {
		return 0, nil, err
	}

	if data == nil {
		return 0, nil, nil
	}

	vals, err := redis.Values(data, err)
	if err != nil {
		return 0, nil, err
	}

	return vals[0].(int), vals[1].([]string), nil
}

type EnumFunc func(idx int, key string) error

////////////////////////////////////////////////////////////////////////////////////////////////
// EnumerateKeys
////////////////////////////////////////////////////////////////////////////////////////////////
func (s *Store) EnumerateKeys(match string, enumFunc EnumFunc) error {

	var (
		cursor = 0
		res    []string
		err    error
	)

	for cursor != -1 {
		cursor, res, err = s.Enumerate(cursor, match, 10)
		if err != nil {
			return err
		}

		if len(res) > 0 {
			for idx, key := range res {
				err := enumFunc(idx, key)
				if err != nil {
					return err
				}
			}
		}
	}

	return nil
}

////////////////////////////////////////////////////////////////////////////////////////////////
// Decode Values from Redis Response
////////////////////////////////////////////////////////////////////////////////////////////////
func (s *Store) DecodeValues(values []interface{}) ([]interface{}, error) {

	if values == nil {
		return nil, nil
	}
	out := make([]interface{}, len(values))

	for n, val := range values {
		if err := msgpack.Unmarshal(val.([]byte), &out[n]); err != nil {
			return nil, err
		}
	}

	return out, nil
}
