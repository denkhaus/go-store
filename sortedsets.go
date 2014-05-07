package store

import (
	"github.com/garyburd/redigo/redis"
	"github.com/vmihailenco/msgpack"
	"strconv"
)

////////////////////////////////////////////////////////////////////////////////////////////////
// Sets score and value in a SortedSet
////////////////////////////////////////////////////////////////////////////////////////////////
func (s *Store) SortedSetSet(set string, score float64, value interface{}) (int, error) {
	conn := s.Pool.Get()
	defer conn.Close()

	if err := conn.Err(); err != nil {
		return 0, err
	}

	b, err := msgpack.Marshal(value)
	if err != nil {
		return 0, err
	}

	sc := strconv.FormatFloat(score, 'g', -1, 64)

	data, err := conn.Do("ZADD", set, sc, b)
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
// Sets score and value in a SortedSet
////////////////////////////////////////////////////////////////////////////////////////////////
func (s *Store) SortedSetSize(set string, scoreMin float64, scoreMax float64) (int, error) {
	conn := s.Pool.Get()
	defer conn.Close()

	if err := conn.Err(); err != nil {
		return 0, err
	}

	scMin := strconv.FormatFloat(scoreMin, 'g', -1, 64)
	scMax := strconv.FormatFloat(scoreMax, 'g', -1, 64)

	data, err := conn.Do("ZCOUNT", set, scMin, scMax)
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
// Returns values from SortedSet between the specified scores, same as SortedSetGetAsc.
////////////////////////////////////////////////////////////////////////////////////////////////
func (s *Store) SortedSetGet(set string, scoreMin float64, scoreMax float64) ([]interface{}, error) {
	return s.SortedSetGetAsc(set, scoreMin, scoreMax)
}

////////////////////////////////////////////////////////////////////////////////////////////////
// Returns the specified range of elements in the sorted set stored at set. The elements are considered
// to be ordered from the lowest to the highest score. Ascending lexicographical
// order is used for elements with equal score.
////////////////////////////////////////////////////////////////////////////////////////////////
func (s *Store) SortedSetGetAsc(set string, scoreMin float64, scoreMax float64) ([]interface{}, error) {
	conn := s.Pool.Get()
	defer conn.Close()

	if err := conn.Err(); err != nil {
		return nil, err
	}

	scMin := strconv.FormatFloat(scoreMin, 'g', -1, 64)
	scMax := strconv.FormatFloat(scoreMax, 'g', -1, 64)

	data, err := conn.Do("ZRANGE", set, scMin, scMax)
	if err != nil {
		return nil, err
	}

	if data == nil {
		return nil, nil
	}

	res, err := redis.Values(data, err)
	if err != nil {
		return nil, err
	}

	return s.DecodeValues(res)
}

////////////////////////////////////////////////////////////////////////////////////////////////
// Returns the specified range of elements in the sorted set stored at set. The elements are considered
// to be ordered from the highest to the lowest score. Descending lexicographical
// order is used for elements with equal score.
////////////////////////////////////////////////////////////////////////////////////////////////
func (s *Store) SortedSetGetDesc(set string, scoreMin float64, scoreMax float64) ([]interface{}, error) {
	conn := s.Pool.Get()
	defer conn.Close()

	if err := conn.Err(); err != nil {
		return nil, err
	}

	scMin := strconv.FormatFloat(scoreMin, 'g', -1, 64)
	scMax := strconv.FormatFloat(scoreMax, 'g', -1, 64)

	data, err := conn.Do("ZREVRANGE", set, scMin, scMax)
	if err != nil {
		return nil, err
	}

	if data == nil {
		return nil, nil
	}

	res, err := redis.Values(data, err)
	if err != nil {
		return nil, err
	}

	return s.DecodeValues(res)
}

///////////////////////////////////////////////////////////////////////////////////////////////
// Removes all elements in the sorted set stored at key with a score between min and max (inclusive).
// Returns the number of elements removed.
////////////////////////////////////////////////////////////////////////////////////////////////
func (s *Store) SortedSetDeleteByScore(key string, scoreMin float64, scoreMax float64) (int, error) {
	conn := s.Pool.Get()
	defer conn.Close()

	if err := conn.Err(); err != nil {
		return 0, err
	}

	scMin := strconv.FormatFloat(scoreMin, 'g', -1, 64)
	scMax := strconv.FormatFloat(scoreMax, 'g', -1, 64)

	data, err := conn.Do("ZREMRANGEBYSCORE", key, scMin, scMax)
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

///////////////////////////////////////////////////////////////////////////////////////////////
// Removes all elements in the sorted set stored at key.
// Returns the number of elements removed.
////////////////////////////////////////////////////////////////////////////////////////////////
func (s *Store) SortedSetDeleteAll(key string) (int, error) {
	return s.SortedSetDeleteByScore(key, 0, -1)
}

///////////////////////////////////////////////////////////////////////////////////////////////
// Get all elements in the sorted set stored at key.
// Returns the number of elements removed.
////////////////////////////////////////////////////////////////////////////////////////////////
func (s *Store) SortedSetGetAll(key string) ([]interface{}, error) {
	return s.SortedSetGet(key, 0, -1)
}
