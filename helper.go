package store

import (
//"github.com/garyburd/redigo/redis"
//"github.com/vmihailenco/msgpack"
//"time"
)

type KeysEnumFunc func(key string) error

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
			err := enumerate(key)
			if err != nil {
				return err
			}
		}
	}

	return nil
}
