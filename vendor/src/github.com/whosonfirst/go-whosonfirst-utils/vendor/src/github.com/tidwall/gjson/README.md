<p align="center">
<img 
    src="logo.png" 
    width="240" height="78" border="0" alt="GJSON">
<br>
<a href="https://travis-ci.org/tidwall/gjson"><img src="https://img.shields.io/travis/tidwall/gjson.svg?style=flat-square" alt="Build Status"></a>
<a href="https://godoc.org/github.com/tidwall/gjson"><img src="https://img.shields.io/badge/api-reference-blue.svg?style=flat-square" alt="GoDoc"></a>
</p>

<p align="center">get a json value quickly</a></p>

GJSON is a Go package that provides a [fast](#performance) and [simple](#get-a-value) way to get values from a json document.
It has features such as [one line retrieval](#get-a-value), [dot notation paths](#path-syntax), [iteration](#iterate-through-an-object-or-array). It can also [unmarshal](#unmarshalling) 3 to 4 times faster than the standard Go `encoding/json` unmarshaller.

Getting Started
===============

## Installing

To start using GJSON, install Go and run `go get`:

```sh
$ go get -u github.com/tidwall/gjson
```

This will retrieve the library.

## Get a value
Get searches json for the specified path. A path is in dot syntax, such as "name.last" or "age". This function expects that the json is well-formed. Bad json will not panic, but it may return back unexpected results. When the value is found it's returned immediately. 

```go
package main

import "github.com/tidwall/gjson"

const json = `{"name":{"first":"Janet","last":"Prichard"},"age":47}`

func main() {
	value := gjson.Get(json, "name.last")
	println(value.String())
}
```

This will print:

```
Prichard
```
*There's also the [GetMany](#get-multiple-values-at-once) function to get multiple values at once, and [GetBytes](#working-with-bytes) for working with JSON byte slices.*

## Path Syntax

A path is a series of keys separated by a dot.
A key may contain special wildcard characters '\*' and '?'.
To access an array value use the index as the key.
To get the number of elements in an array or to access a child path, use the '#' character.
The dot and wildcard characters can be escaped with '\\'.

```json
{
  "name": {"first": "Tom", "last": "Anderson"},
  "age":37,
  "children": ["Sara","Alex","Jack"],
  "fav.movie": "Deer Hunter",
  "friends": [
    {"first": "Dale", "last": "Murphy", "age": 44},
    {"first": "Roger", "last": "Craig", "age": 68},
    {"first": "Jane", "last": "Murphy", "age": 47}
  ]
}
```
```
"name.last"          >> "Anderson"
"age"                >> 37
"children"           >> ["Sara","Alex","Jack"]
"children.#"         >> 3
"children.1"         >> "Alex"
"child*.2"           >> "Jack"
"c?ildren.0"         >> "Sara"
"fav\.movie"         >> "Deer Hunter"
"friends.#.first"    >> ["Dale","Roger","Jane"]
"friends.1.last"     >> "Craig"
```

You can also query an array for the first match by using `#[...]`, or find all matches with `#[...]#`. 
Queries support the `==`, `!=`, `<`, `<=`, `>`, `>=` comparison operators and the simple pattern matching `%` operator.

```
friends.#[last=="Murphy"].first    >> "Dale"
friends.#[last=="Murphy"]#.first   >> ["Dale","Jane"]
friends.#[age>45]#.last            >> ["Craig","Murphy"]
friends.#[first%"D*"].last         >> "Murphy"
```

## Result Type

GJSON supports the json types `string`, `number`, `bool`, and `null`. 
Arrays and Objects are returned as their raw json types. 

The `Result` type holds one of these:

```
bool, for JSON booleans
float64, for JSON numbers
string, for JSON string literals
nil, for JSON null
```

To directly access the value:

```go
result.Type    // can be String, Number, True, False, Null, or JSON
result.Str     // holds the string
result.Num     // holds the float64 number
result.Raw     // holds the raw json
result.Index   // index of raw value in original json, zero means index unknown
```

There are a variety of handy functions that work on a result:

```go
result.Exists() bool
result.Value() interface{}
result.Int() int64
result.Uint() uint64
result.Float() float64
result.String() string
result.Bool() bool
result.Time() time.Time
result.Array() []gjson.Result
result.Map() map[string]gjson.Result
result.Get(path string) Result
result.ForEach(iterator func(key, value Result) bool)
result.Less(token Result, caseSensitive bool) bool
```

The `result.Value()` function returns an `interface{}` which requires type assertion and is one of the following Go types:



The `result.Array()` function returns back an array of values.
If the result represents a non-existent value, then an empty array will be returned.
If the result is not a JSON array, the return value will be an array containing one result.

```go
boolean >> bool
number  >> float64
string  >> string
null    >> nil
array   >> []interface{}
object  >> map[string]interface{}
```

## Get nested array values

Suppose you want all the last names from the following json:

```json
{
  "programmers": [
    {
      "firstName": "Janet", 
      "lastName": "McLaughlin", 
    }, {
      "firstName": "Elliotte", 
      "lastName": "Hunter", 
    }, {
      "firstName": "Jason", 
      "lastName": "Harold", 
    }
  ]
}`
```

You would use the path "programmers.#.lastName" like such:

```go
result := gjson.Get(json, "programmers.#.lastName")
for _,name := range result.Array() {
	println(name.String())
}
```

You can also query an object inside an array:

```go
name := gjson.Get(json, `programmers.#[lastName="Hunter"].firstName`)
println(name.String())  // prints "Elliotte"
```

## Iterate through an object or array

The `ForEach` function allows for quickly iterating through an object or array. 
The key and value are passed to the iterator function for objects.
Only the value is passed for arrays.
Returning `false` from an iterator will stop iteration.

```go
result := gjson.Get(json, "programmers")
result.ForEach(func(key, value gjson.Result) bool{
	println(value.String()) 
	return true // keep iterating
})
```

## Simple Parse and Get

There's a `Parse(json)` function that will do a simple parse, and `result.Get(path)` that will search a result.

For example, all of these will return the same result:

```go
gjson.Parse(json).Get("name").Get("last")
gjson.Get(json, "name").Get("last")
gjson.Get(json, "name.last")
```

## Check for the existence of a value

Sometimes you just want to know if a value exists. 

```go
value := gjson.Get(json, "name.last")
if !value.Exists() {
	println("no last name")
} else {
	println(value.String())
}

// Or as one step
if gjson.Get(json, "name.last").Exists(){
	println("has a last name")
}
```

## Unmarshalling

There's a `gjson.Unmarshal` function which loads json data into a value.
It's a drop in replacement for `json.Unmarshal` and you can typically
see a 3-4x boost in performance without the need for external generators.

This function works almost identically to `json.Unmarshal` except that it
expects the json to be well-formed prior to being called. Bad json
will not panic or cause a decoding error.

Another difference is that `gjson.Unmarshal` will automatically attempt to
convert JSON values to any Go type. For example, the JSON string "100" or
the JSON number 100 can be equally assigned to Go string, int, byte, uint64,
etc. This rule applies to all types.


```go
package main

import (
	"fmt"

	"github.com/tidwall/gjson"
)

type Animal struct {
	Type  string `json:"type"`
	Sound string `json:"sound"`
	Age   int    `json:"age"`
}

var json = `{
	"type": "Dog",
	"Sound": "Bark",
	"Age": "11"
}`

func main() {
	var dog Animal
	gjson.Unmarshal([]byte(json), &dog)
	fmt.Printf("type: %s, sound: %s, age: %d\n", dog.Type, dog.Sound, dog.Age)
}
```

This will print:

```
type: Dog, sound: Bark, age: 11
```

## Unmarshal to a map

To unmarshal to a `map[string]interface{}`:

```go
m, ok := gjson.Parse(json).Value().(map[string]interface{})
if !ok{
	// not a map
}
```

## Working with Bytes

If your JSON is contained in a `[]byte` slice, there's the [GetBytes](https://godoc.org/github.com/tidwall/gjson#GetBytes) function. This is preferred over `Get(string(data), path)`.

```go
var json []byte = ...
result := gjson.GetBytes(json, path)
```

If you are using the `gjson.GetBytes(json, path)` function and you want to avoid converting `result.Raw` to a `[]byte`, then you can use this pattern:

```go
var json []byte = ...
result := gjson.GetBytes(json, path)
var raw []byte
if result.Index > 0 {
    raw = json[result.Index:result.Index+len(result.Raw)]
} else {
    raw = []byte(result.Raw)
}
```

This is a best-effort no allocation sub slice of the original json. This method utilizes the `result.Index` field, which is the position of the raw data in the original json. It's possible that the value of `result.Index` equals zero, in which case the `result.Raw` is converted to a `[]byte`.

## Get multiple values at once

The `GetMany` function can be used to get multiple values at the same time, and is optimized to scan over a JSON payload once.

```go
results := gjson.GetMany(json, "name.first", "name.last", "age")
```

The return value is a `[]Result`, which will always contain exactly the same number of items as the input paths.

## Performance

Benchmarks of GJSON alongside [encoding/json](https://golang.org/pkg/encoding/json/), 
[ffjson](https://github.com/pquerna/ffjson), 
[EasyJSON](https://github.com/mailru/easyjson),
[jsonparser](https://github.com/buger/jsonparser),
and [json-iterator](https://github.com/json-iterator/go)

```
BenchmarkGJSONGet-8                  3000000        372 ns/op          0 B/op         0 allocs/op
BenchmarkGJSONUnmarshalMap-8          900000       4154 ns/op       1920 B/op        26 allocs/op
BenchmarkJSONUnmarshalMap-8           600000       9019 ns/op       3048 B/op        69 allocs/op
BenchmarkJSONUnmarshalStruct-8        600000       9268 ns/op       1832 B/op        69 allocs/op
BenchmarkJSONDecoder-8                300000      14120 ns/op       4224 B/op       184 allocs/op
BenchmarkFFJSONLexer-8               1500000       3111 ns/op        896 B/op         8 allocs/op
BenchmarkEasyJSONLexer-8             3000000        887 ns/op        613 B/op         6 allocs/op
BenchmarkJSONParserGet-8             3000000        499 ns/op         21 B/op         0 allocs/op
BenchmarkJSONIterator-8              3000000        812 ns/op        544 B/op         9 allocs/op
```

Benchmarks for the `GetMany` function:

```
BenchmarkGJSONGetMany4Paths-8        4000000       303 ns/op         112 B/op         0 allocs/op
BenchmarkGJSONGetMany8Paths-8        8000000       208 ns/op          56 B/op         0 allocs/op
BenchmarkGJSONGetMany16Paths-8      16000000       156 ns/op          56 B/op         0 allocs/op
BenchmarkGJSONGetMany32Paths-8      32000000       127 ns/op          64 B/op         0 allocs/op
BenchmarkGJSONGetMany64Paths-8      64000000       117 ns/op          64 B/op         0 allocs/op
BenchmarkGJSONGetMany128Paths-8    128000000       109 ns/op          64 B/op         0 allocs/op
```

JSON document used:

```json
{
  "widget": {
    "debug": "on",
    "window": {
      "title": "Sample Konfabulator Widget",
      "name": "main_window",
      "width": 500,
      "height": 500
    },
    "image": { 
      "src": "Images/Sun.png",
      "hOffset": 250,
      "vOffset": 250,
      "alignment": "center"
    },
    "text": {
      "data": "Click Here",
      "size": 36,
      "style": "bold",
      "vOffset": 100,
      "alignment": "center",
      "onMouseUp": "sun1.opacity = (sun1.opacity / 100) * 90;"
    }
  }
}    
```

Each operation was rotated though one of the following search paths:

```
widget.window.name
widget.image.hOffset
widget.text.onMouseUp
```

For the `GetMany` benchmarks these paths are used:

```
widget.window.name
widget.image.hOffset
widget.text.onMouseUp
widget.window.title
widget.image.alignment
widget.text.style
widget.window.height
widget.image.src
widget.text.data
widget.text.size
```

*These benchmarks were run on a MacBook Pro 15" 2.8 GHz Intel Core i7 using Go 1.8.*

## Contact
Josh Baker [@tidwall](http://twitter.com/tidwall)

## License

GJSON source code is available under the MIT [License](/LICENSE).