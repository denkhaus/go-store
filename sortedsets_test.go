
package store

import (
	"fmt"
	"github.com/denkhaus/tcgl/asserts"
	"testing"
)

/////////////////////////////////////////////////////////////////////////////////////////////////////
// TestHashSetGet
/////////////////////////////////////////////////////////////////////////////////////////////////////
func TestSortedSets(t *testing.T) {
	assert := asserts.NewTestingAsserts(t, true)
	st := createStore(t)
	defer st.Close()

	set := "testSet"

	err := st.Delete(set)
	assert.Nil(err, "Error should be nil.")

	for i := 0; i < 5; i++ {
		value := fmt.Sprintf("setvalue%d", i)

		res, err := st.SortedSetSet(set, float64(i), value)
		assert.Nil(err, "Error should be nil.")

		if res != 1 {
			t.Error("sortedset unexpected return value :: ", res)
			t.Fail()
		}
	}

	sSize, err := st.SortedSetSize(set, float64(0), float64(5))
	assert.Nil(err, "Error should be nil.")

	if sSize != 5 {
		t.Error(fmt.Sprintf("invalid SortedSetSize, expected 5, result ::%d ", sSize))
		t.Fail()
	}

	for i := 0; i < 5; i++ {
		value := fmt.Sprintf("setvalue%d", i)
		res, err := st.SortedSetGet(set, float64(i), float64(i))
		assert.Nil(err, "Error should be nil.")

		if len(res) != 1 || res[0].(string) != value {
			t.Error(fmt.Sprintf("sortedsetget: wrong value %s, expected %s", res, value))
			t.Fail()
		}

		sSize, err := st.SortedSetSize(set, float64(i), float64(i))
		assert.Nil(err, "Error should be nil.")

		if sSize != 1 {
			t.Error(fmt.Sprintf("invalid SortedSetSize, expected 1, result ::%d ", sSize))
			t.Fail()
		}
	}

	res, err := st.SortedSetDeleteByScore(set, float64(0), float64(5))
	assert.Nil(err, "Error should be nil.")

	if res != 5 {
		t.Error(fmt.Sprintf("invalid SortedSetDeleteByScore response, expected 5, result ::%d ", sSize))
		t.Fail()
	}
}