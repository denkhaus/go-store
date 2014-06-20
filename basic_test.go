package store

import (
	"fmt"
	"github.com/denkhaus/tcgl/asserts"
	"testing"
)

/////////////////////////////////////////////////////////////////////////////////////////////////////
// TestSetGet
/////////////////////////////////////////////////////////////////////////////////////////////////////
func TestBasicSetGet(t *testing.T) {
	assert := asserts.NewTestingAsserts(t, true)
	st := createStore(t)
	defer st.Close()

	key := "testKey1"
	value := "This is a test"

	err := st.Delete(key)
	assert.Nil(err, "Error should be nil.")

	err = st.Set(key, value)
	assert.Nil(err, "Error should be nil.")

	res, err := st.Get(key)
	assert.Nil(err, "Error should be nil.")
	assert.Equal(res, value, "TestGetSet wrong value")
}

/////////////////////////////////////////////////////////////////////////////////////////////////////
// TestSetGet
/////////////////////////////////////////////////////////////////////////////////////////////////////
func TestEnumerate(t *testing.T) {
	assert := asserts.NewTestingAsserts(t, true)
	st := createStore(t)
	defer st.Close()

	keyBase := "testEnumerate%d"
	valueBase := "enumerateTestValue%d"

	for i := 5; i > 0; i-- {
		key := fmt.Sprintf(keyBase, i)
		value := fmt.Sprintf(valueBase, i)
		err := st.Set(key, value)
		assert.Nil(err, "Error should be nil.")
	}

	cursor, res, err := st.Enumerate(0, "testEnu*", 5)
	assert.Nil(err, "Error should be nil.")
	assert.Length(res, 5, "enumerate res return wrong")
	assert.Equal(cursor, 0, "enumerate cursor return wrong")
}
