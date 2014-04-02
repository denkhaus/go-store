package store

import (
	"fmt"
	"testing"
)

/////////////////////////////////////////////////////////////////////////////////////////////////////
// TestSetGet
/////////////////////////////////////////////////////////////////////////////////////////////////////
func TestBasicSetGet(t *testing.T) {
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
