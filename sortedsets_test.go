package store

import (
	"fmt"
	"testing"
)

/////////////////////////////////////////////////////////////////////////////////////////////////////
// TestHashSetGet
/////////////////////////////////////////////////////////////////////////////////////////////////////
func TestSortedSets(t *testing.T) {
	st := createStore(t)
	defer st.Close()

	set := "testSet"

	if err := st.Delete(set); err != nil {
		t.Error("cannot delete testSet :: ", err.Error())
		t.Fail()
	}

	for i := 0; i < 5; i++ {
		value := fmt.Sprintf("setvalue%d", i)

		res, err := st.SortedSetSet(set, float64(i), value)

		if err != nil {
			t.Error("sortedset set error :: ", err.Error())
			t.Fail()
		}

		if res != 1 {
			t.Error("sortedset unexpected return value :: ", res)
			t.Fail()
		}
	}

	sSize, err := st.SortedSetSize(set, float64(0), float64(5))

	if err != nil {
		t.Error("sortedset size error1 :: ", err.Error())
		t.Fail()
	}

	if sSize != 5 {
		t.Error(fmt.Sprintf("invalid SortedSetSize, expected 5, result ::%d ", sSize))
		t.Fail()
	}

	for i := 0; i < 5; i++ {
		value := fmt.Sprintf("setvalue%d", i)
		res, err := st.SortedSetGet(set, float64(i), float64(i))

		if err != nil {
			t.Error("sortedset get error :: ", err.Error())
			t.Fail()
		}

		if len(res) != 1 || res[0].(string) != value {
			t.Error(fmt.Sprintf("sortedsetget: wrong value %s, expected %s", res, value))
			t.Fail()
		}

		sSize, err := st.SortedSetSize(set, float64(i), float64(i))

		if err != nil {
			t.Error("sortedset size error2 :: ", err.Error())
			t.Fail()
		}

		if sSize != 1 {
			t.Error(fmt.Sprintf("invalid SortedSetSize, expected 1, result ::%d ", sSize))
			t.Fail()
		}
	}

	res, err := st.SortedSetDeleteByScore(set, float64(0), float64(5))

	if err != nil {
		t.Error("sortedset delete error1 :: ", err.Error())
		t.Fail()
	}

	if res != 5 {
		t.Error(fmt.Sprintf("invalid SortedSetDeleteByScore response, expected 5, result ::%d ", sSize))
		t.Fail()
	}
}
