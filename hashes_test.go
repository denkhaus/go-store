package store

import (
	"fmt"
	"github.com/denkhaus/tcgl/asserts"
	"testing"
)

/////////////////////////////////////////////////////////////////////////////////////////////////////
// TestHashSetGet
/////////////////////////////////////////////////////////////////////////////////////////////////////
func TestHashSetGetSizeGetFieldsGetValues(t *testing.T) {
	assert := asserts.NewTestingAsserts(t, true)
	st := createStore(t)
	defer st.Close()

	hash := "testHash"

	err := st.Delete(hash)
	assert.Nil(err, "Error should be nil.")

	for i := 0; i < 5; i++ {
		field := fmt.Sprintf("hashfield%d", i)
		value := fmt.Sprintf("hashvalue%d", i)

		err := st.HashSet(hash, field, value)
		assert.Nil(err, "Error should be nil.")
	}

	hSize, err := st.HashSize(hash)
	assert.Nil(err, "Error should be nil.")
	assert.Equal(hSize, 5, "invalid HashSize")

	for i := 0; i < 5; i++ {
		field := fmt.Sprintf("hashfield%d", i)
		value := fmt.Sprintf("hashvalue%d", i)

		res, err := st.HashGet(hash, field)

		assert.Nil(err, "Error should be nil.")
		assert.Equal(res, value, "hashget: wrong value")
	}

	res1, err := st.HashGetFields(hash)
	assert.Nil(err, "Error should be nil.")
	assert.Length(res1, 5, "invalid hash get fields result")

	res2, err := st.HashGetValues(hash)
	assert.Nil(err, "Error should be nil.")

	if res2 == nil || len(res2) != 5 {
		t.Error(fmt.Sprintf("invalid hash get values result %+v", res2))
		t.Fail()
	}

	for i := 0; i < 5; i++ {
		field := fmt.Sprintf("hashfield%d", i)
		value := fmt.Sprintf("hashvalue%d", i)

		if res1[i] != field {
			t.Error(fmt.Sprintf("hashgetkey: wrong value %s, expected %s", res1[i], field))
			t.Fail()
		}

		if res2[i] != value {
			t.Error(fmt.Sprintf("hashgetvalue: wrong value %s, expected %s", res2[i], field))
			t.Fail()
		}
	}

	for i := 0; i < 5; i++ {

		hSize1, err := st.HashSize(hash)
		assert.Nil(err, "Error should be nil.")

		field := fmt.Sprintf("hashfield%d", i)

		_, err = st.HashDeleteField(hash, field)
		assert.Nil(err, "Error should be nil.")

		hSize2, err := st.HashSize(hash)
		assert.Nil(err, "Error should be nil.")

		if hSize2 >= hSize1 {
			t.Error(fmt.Sprintf("invalid HashSize, values %d, %d ", hSize1, hSize2))
			t.Fail()
		}
	}
}
