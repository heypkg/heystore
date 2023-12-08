package jsontype

import (
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"time"
)

type JSONTime time.Time

func (t JSONTime) Format() string {
	_t := time.Time(t)
	return _t.UTC().Format(time.RFC3339)
}

func (t JSONTime) MarshalJSON() ([]byte, error) {
	_t := time.Time(t)
	if _t.IsZero() {
		return []byte("-1"), nil
	}
	return []byte(fmt.Sprintf("%d", _t.UnixNano()/1000)), nil
}

func (t *JSONTime) UnmarshalJSON(data []byte) error {
	var ts int64
	err := json.Unmarshal(data, &ts)
	if err != nil {
		return err
	}
	*t = JSONTime(time.Unix(0, ts*int64(time.Microsecond)))
	return nil
}

func (t JSONTime) Value() (driver.Value, error) {
	return time.Time(t), nil
}

func (t *JSONTime) Scan(value interface{}) error {
	switch v := value.(type) {
	case time.Time:
		*t = JSONTime(v)
	case []byte:
		tt, err := time.Parse(time.RFC3339Nano, string(v))
		if err != nil {
			return err
		}
		*t = JSONTime(tt)
	case string:
		tt, err := time.Parse(time.RFC3339Nano, v)
		if err != nil {
			return err
		}
		*t = JSONTime(tt)
	default:
		return fmt.Errorf("unsupported type for JSONTime: %T", value)
	}
	return nil
}
