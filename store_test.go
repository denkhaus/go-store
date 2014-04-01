package store

import (
	"fmt"
	"testing"
)

/////////////////////////////////////////////////////////////////////////////////////////////////////
//
/////////////////////////////////////////////////////////////////////////////////////////////////////
func createStore(t *testing.T) *Store {
	st, err := NewStore(10, "tcp", ":6379", "")

	if err != nil {
		t.Error("testing store: cannot create store :: ", err.Error())
		t.Fail()
	}

	return st
}

/////////////////////////////////////////////////////////////////////////////////////////////////////
// TestSetGet
/////////////////////////////////////////////////////////////////////////////////////////////////////
func TestSetGet(t *testing.T) {
	st := createStore(t)
	defer st.Close()

	key := "testKey1"
	value := "This is a test"

	if err := st.Delete(key); err != nil {
		t.Error("cannot delete testKey :: ", err.Error())
		t.Fail()
	}

	if err := st.Set(key, value); err != nil {
		t.Error("set error :: ", err.Error())
		t.Fail()
	}

	res, err := st.Get(key)

	if err != nil {
		t.Error("get error :: ", err.Error())
		t.Fail()
	}

	if res != value {
		fmt.Println("TestGetSet wrong expected value::" + res.(string))
		t.Fail()
	}
}

/////////////////////////////////////////////////////////////////////////////////////////////////////
// TestHashSetGet
/////////////////////////////////////////////////////////////////////////////////////////////////////
func TestHashSetGet(t *testing.T) {
	st := createStore(t)
	defer st.Close()

	hash := "testHash"

	if err := st.Delete(hash); err != nil {
		t.Error("cannot delete testHash :: ", err.Error())
		t.Fail()
	}

	for i := 0; i < 5; i++ {
		field := fmt.Sprintf("hashfield%d", i)
		value := fmt.Sprintf("hashvalue%d", i)

		if err := st.HashSet(hash, field, value); err != nil {
			t.Error("hash set error :: ", err.Error())
			t.Fail()
		}
	}

	hSize, err := st.HashSize(hash)

	if err != nil {
		t.Error("hash size error1 :: ", err.Error())
		t.Fail()
	}

	if hSize != 5 {
		t.Error(fmt.Sprintf("invalid HashSize, expected 5, result ::%d ", hSize))
		t.Fail()
	}

	for i := 0; i < 5; i++ {
		field := fmt.Sprintf("hashfield%d", i)
		value := fmt.Sprintf("hashvalue%d", i)
		res, err := st.HashGet(hash, field)

		if err != nil {
			t.Error("hash get error :: ", err.Error())
			t.Fail()
		}

		if res != value {
			t.Error(fmt.Sprintf("hashget: wrong value %s, expected %s", res, value))
			t.Fail()
		}
	}

	res1, err := st.HashGetKeys(hash)

	if err != nil {
		t.Error("hash get keys error :: ", err.Error())
		t.Fail()
	}

	if res1 == nil || len(res1) != 5 {
		t.Error(fmt.Sprintf("invalid hash get keys result %+v", res1))
		t.Fail()
	}

	res2, err := st.HashGetValues(hash)

	if err != nil {
		t.Error("hash get values error :: ", err.Error())
		t.Fail()
	}

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

		if err != nil {
			t.Error("hash size error2 :: ", err.Error())
			t.Fail()
		}

		field := fmt.Sprintf("hashfield%d", i)
		_, err = st.HashDeleteField(hash, field)

		if err != nil {
			t.Error("hashdeletefield error :: ", err.Error())
			t.Fail()
		}

		hSize2, err := st.HashSize(hash)

		if err != nil {
			t.Error("hash size error3 :: ", err.Error())
			t.Fail()
		}

		if hSize2 >= hSize1 {
			t.Error(fmt.Sprintf("invalid HashSize, values %d, %d ", hSize1, hSize2))
			t.Fail()
		}
	}
}
