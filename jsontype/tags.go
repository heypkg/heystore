package jsontype

import (
	"reflect"

	"github.com/spf13/cast"
)

type Tags map[string]any

func StringArrayToTags(values []string) Tags {
	tags := Tags{}
	for k, v := range values {
		tags[v] = k
	}
	return tags
}

func IntMapToTags(values map[string]int) Tags {
	tags := Tags{}
	for k, v := range values {
		tags[k] = v
	}
	return tags
}

func StringMapToTags(values map[string]string) Tags {
	tags := Tags{}
	for k, v := range values {
		tags[k] = v
	}
	return tags
}

func BoolMapToTags(values map[string]bool) Tags {
	tags := Tags{}
	for k, v := range values {
		tags[k] = v
	}
	return tags
}

func (m Tags) Set(name string, v any) {
	if v == nil || name == "" {
		return
	}
	vv := reflect.ValueOf(v)
	if vv.Kind() == reflect.Ptr {
		if vv.IsNil() {
			return
		}
		v = vv.Interface()

	}
	// utils.GetLogger().Warn("debug", zap.Any(name, v))

	m[name] = v
}

func (m Tags) GetString(name string) string {
	v := m[name]
	return cast.ToString(v)
}

// func (m Tags) SetStruct(obj interface{}) {
// 	if obj == nil {
// 		return
// 	}
// 	value := reflect.ValueOf(obj)
// 	if value.Kind() == reflect.Ptr {
// 		value = value.Elem()
// 	}
// 	for i := 0; i < value.NumField(); i++ {
// 		fieldType := value.Type().Field(i)
// 		fieldValue := value.Field(i)
// 		if !fieldValue.IsValid() {
// 			continue
// 		}
// 		if fieldValue.Kind() == reflect.Ptr {
// 			fieldValue = fieldValue.Elem()
// 		}
// 		fieldName := fieldType.Name
// 		if fieldValue.IsValid() {
// 			m[fieldName] = fieldValue.Interface()
// 		}
// 	}
// }
