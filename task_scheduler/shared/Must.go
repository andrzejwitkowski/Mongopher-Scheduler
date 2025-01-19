package shared

import (
    "bytes"
    "runtime"
    "strconv"
)

func Must[T any](value T, err error) T {
    if err != nil {
        panic(err)
    }
    return value
}

func MustOk(err error) {
    if err != nil {
        panic(err)
    }
}

func GoroutineID() uint64 {
    b := make([]byte, 64)
    b = b[:runtime.Stack(b, false)]
    b = bytes.TrimPrefix(b, []byte("goroutine "))
    b = b[:bytes.IndexByte(b, ' ')]
    n, _ := strconv.ParseUint(string(b), 10, 64)
    return n
}