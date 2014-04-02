package store

import (
//"github.com/garyburd/redigo/redis"
//"github.com/vmihailenco/msgpack"
//"time"
)

type KeysEnumFunc func(key string) error
type ValuesEnumFunc func(value interface{}) error

////////////////////////////////////////////////////////////////////////////////////////////////
// Get all Keys from a hash. If the Hash is not available or empty, it will return nil, nil.
////////////////////////////////////////////////////////////////////////////////////////////////
func (s *Store) HashEnumerateKeys(hash string, enumerate KeysEnumFunc) error {

	keys, err := s.HashGetKeys(hash)
	if err != nil {
		return err
	}

	if keys != nil {
		for _, key := range keys {
			if err := enumerate(key); err != nil {
				return err
			}
		}
	}

	return nil
}

////////////////////////////////////////////////////////////////////////////////////////////////
// Get all Keys from a hash. If the Hash is not available or empty, it will return nil, nil.
////////////////////////////////////////////////////////////////////////////////////////////////
func (s *Store) HashEnumerateValues(hash string, enumerate ValuesEnumFunc) error {

	vals, err := s.HashGetValues(hash)
	if err != nil {
		return err
	}

	if vals != nil {
		for _, val := range values {
			if err := enumerate(val); err != nil {
				return err
			}
		}
	}

	return nil
}
