package store

import (
	"github.com/garyburd/redigo/redis"
	"github.com/vmihailenco/msgpack"
	"strconv"
)

////////////////////////////////////////////////////////////////////////////////////////////////
//
////////////////////////////////////////////////////////////////////////////////////////////////
func (s *Store) SetSet(set string, member string) (int, error) {
	conn := s.Pool.Get()
	defer conn.Close()

	if err := conn.Err(); err != nil {
		return 0, err
	}

	data, err := conn.Do("SADD", set, member)
	return redis.Int(data, err)
}

////////////////////////////////////////////////////////////////////////////////////////////////
//
////////////////////////////////////////////////////////////////////////////////////////////////
func (s *Store) SetDelete(set string, member string) (int, error) {
	conn := s.Pool.Get()
	defer conn.Close()

	if err := conn.Err(); err != nil {
		return nil, err
	}

	data, err := conn.Do("SREM", set, member)
	return redis.Int(data, err)
}
