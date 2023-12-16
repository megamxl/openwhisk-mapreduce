# ow

Utility for running Go language Actions on OpenWhisk, see [this project](https://github.com/jthomas/openwhisk_go_action).

install
--
```
go get github.com/jthomas/ow
```

usage
--

``` go 
package main

import (
	"encoding/json"
	"github.com/jthomas/ow"
)

type Params struct {
	Payload string `json:"payload"`
}

type Result struct {
	Reversed string `json:"reversed"`
}

func reverse_string(to_reverse string) string {
	chars := []rune(to_reverse)
	for i, j := 0, len(chars)-1; i < j; i, j = i+1, j-1 {
		chars[i], chars[j] = chars[j], chars[i]
	}
	return string(chars)
}

func main() {
	ow.RegisterAction(func(value json.RawMessage) (interface{}, error) {
		var params Params
		err := json.Unmarshal(value, &params)
		if err != nil {
			return nil, err
		}
		return Result{Reversed: reverse_string(params.Payload)}, nil
	})
}
```
