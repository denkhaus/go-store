package store

import (
	"bitbucket.org/mendsley/tcgl/asserts"
	"testing"
)

/////////////////////////////////////////////////////////////////////////////////////////////////////
// Create a Basic Store for testing
/////////////////////////////////////////////////////////////////////////////////////////////////////
func createStore(t *testing.T) *Store {
	assert := asserts.NewTestingAsserts(t, true)

	st, err := NewStore(10, "tcp", ":6379", "")
	assert.Nil(err, "Error should be nil.")

	return st
}
