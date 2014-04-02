package store

import (
//"github.com/garyburd/redigo/redis"
//"github.com/vmihailenco/msgpack"
//"time"
)

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
