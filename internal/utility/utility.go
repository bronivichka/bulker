// package utility contains common utilities for use in the project
package utility

import (
	"errors"
	"github.com/gorilla/rpc/v2/json2"
	"os"
	"strconv"
)

var (
	E_INTERNAL    *json2.Error = &json2.Error{Code: json2.E_INTERNAL, Message: "Internal Error"}
	E_INVALID_REQ *json2.Error = &json2.Error{Code: json2.E_INVALID_REQ, Message: "Invalid Request"}
)

// StrVal - convert val to string
func StrVal(val interface{}) string {
	switch val.(type) {
	case nil:
		return ""
	case int:
		return strconv.Itoa(val.(int))
	case int32:
		return strconv.Itoa(int(val.(int32)))
	case int64:
		return strconv.FormatInt(val.(int64), 10)
	case float64:
		return strconv.FormatFloat(val.(float64), 'f', 0, 64)
	case string:
		return val.(string)
	case bool:
		return strconv.FormatBool(val.(bool))
	default:
		return ""
	}
} // StrVal

// Int64Val - convert val to int64
func Int64Val(val interface{}) int64 {
	switch val.(type) {
	case nil:
		return 0
	case int:
		return int64(val.(int))
	case int32:
		return int64(val.(int32))
	case int64:
		return val.(int64)
	case float64:
		return int64(val.(float64))
	case string:
		var i int
		var err error
		if i, err = strconv.Atoi(val.(string)); err != nil {
			return 0
		}
		return int64(i)
	default:
		return 0
	}
} // Int64Val

// IntVal - convert val to int
func IntVal(val interface{}) int {
	switch val.(type) {
	case nil:
		return 0
	case int:
		return val.(int)
	case int32:
		return int(val.(int32))
	case int64:
		return int(val.(int64))
	case float64:
		return int(val.(float64))
	case string:
		var i int
		var err error
		if i, err = strconv.Atoi(val.(string)); err != nil {
			return 0
		}
		return i
	default:
		return 0
	}
} // IntVal

// SavePID - save pid file of process
func SavePID(pidfile string) (err error) {
	// Проверим, не существует ли уже PID
	// В данном случае нам надо чтобы f был nil и err был не nil
	if f, _ := os.Stat(pidfile); f != nil {
		return errors.New("PID file " + pidfile + " already exists")
	}

	// Создаем
	var file *os.File
	if file, err = os.Create(pidfile); err != nil {
		return err
	}

	if _, err = file.Write([]byte(strconv.Itoa(os.Getpid()))); err != nil {
		file.Close()
		return err
	}

	file.Sync()
	file.Close()

	return nil
} // SavePID
