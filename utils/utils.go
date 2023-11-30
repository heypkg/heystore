package utils

import (
	"reflect"
	"strings"
)

func GetRawType(source interface{}) reflect.Type {
	rawType := reflect.TypeOf(source)
	if rawType.Kind() == reflect.Ptr {
		rawType = rawType.Elem()
	}
	return rawType
}

func GetRawTypeName(source interface{}) string {
	rawType := GetRawType(source)
	return rawType.String()
}

func GetRawTypeShortName(source interface{}) string {
	name := GetRawTypeName(source)
	parts := strings.Split(name, ".")
	return parts[1]
}
