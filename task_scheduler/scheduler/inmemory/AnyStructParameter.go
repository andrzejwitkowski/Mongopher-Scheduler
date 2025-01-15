package inmemory

import (
	"fmt"
	"reflect"
	"strings"
)

type AnyStructParameter[T any] struct {
	params T
}

func NewAnyStructParameter[T any](params T) *AnyStructParameter[T] {
	return &AnyStructParameter[T]{
		params: params,
	}
}

func (ap *AnyStructParameter[T]) ToMap() (map[string]interface{}, error) {
    v := reflect.ValueOf(ap.params)
    
    // If it's a pointer, get the underlying element
    if v.Kind() == reflect.Ptr {
        v = v.Elem()
    }
    
    // For simple types, create a map with a single "value" key
    switch v.Kind() {
    case reflect.String, reflect.Int, reflect.Int64, reflect.Float64, reflect.Bool:
        return map[string]interface{}{
            "value": v.Interface(),
        }, nil
    case reflect.Struct:
        // Original struct handling code...
        result := make(map[string]interface{})
        t := v.Type()
        for i := 0; i < t.NumField(); i++ {
            field := t.Field(i)
            value := v.Field(i).Interface()
            
            tag := field.Tag.Get("json")
            name := field.Name
            if tag != "" {
                name = strings.Split(tag, ",")[0]
            }
            
            result[name] = value
        }
        return result, nil
    default:
        return nil, fmt.Errorf("unsupported type: %v", v.Kind())
    }
}
