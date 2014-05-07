package store

import (
	//	"github.com/garyburd/redigo/redis"
	"github.com/vmihailenco/msgpack"
	//	"time"
)

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
