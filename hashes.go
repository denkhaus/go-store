package store

import (
	"github.com/garyburd/redigo/redis"
	"github.com/vmihailenco/msgpack"
)

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
// Get all fields from a hash.
////////////////////////////////////////////////////////////////////////////////////////////////
func (s *Store) HashGetFields(hash string) ([]string, error) {
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

	return s.DecodeValues(vals)
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

type FieldsEnumFunc func(field string) error
type ValuesEnumFunc func(value interface{}) error

////////////////////////////////////////////////////////////////////////////////////////////////
// Enumerate all fields from Hash
////////////////////////////////////////////////////////////////////////////////////////////////
func (s *Store) HashEnumerateFields(hash string, enumerate FieldsEnumFunc) error {

	fields, err := s.HashGetFields(hash)
	if err != nil {
		return err
	}

	if fields != nil {
		for _, field := range fields {
			if err := enumerate(field); err != nil {
				return err
			}
		}
	}

	return nil
}

////////////////////////////////////////////////////////////////////////////////////////////////
// Enumerate all values from Hash
////////////////////////////////////////////////////////////////////////////////////////////////
func (s *Store) HashEnumerateValues(hash string, enumerate ValuesEnumFunc) error {

	vals, err := s.HashGetValues(hash)
	if err != nil {
		return err
	}

	if vals != nil {
		for _, val := range vals {
			if err := enumerate(val); err != nil {
				return err
			}
		}
	}

	return nil
}
