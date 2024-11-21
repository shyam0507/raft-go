package internal

var Handlers = map[string]func([]Value, *map[string]string) Value{
	"SET": set,
	"GET": get,
	// "HSET": hset,
	// "HGET": hget,
	// "HGETALL": hgetall,
}

func ping(args []Value) Value {
	if len(args) == 0 {
		return Value{Typ: "string", Str: "PONG"}
	}

	return Value{Typ: "string", Str: args[0].Bulk}
}

func set(args []Value, m *map[string]string) Value {
	if len(args) != 2 {
		return Value{Typ: "error", Str: "ERR wrong number of arguments for 'set' command"}
	}

	// key := args[0].Bulk
	// value := args[1].Bulk

	return Value{Typ: "string", Str: "OK"}
}

func get(args []Value, m *map[string]string) Value {
	if len(args) != 1 {
		return Value{Typ: "error", Str: "ERR wrong number of arguments for 'set' command"}
	}

	key := args[0].Bulk
	m1 := *m
	val, ok := m1[key]

	if !ok {
		return Value{Typ: "null"}
	}

	return Value{Typ: "bulk", Bulk: val}

}

/*
var mapMap = map[string]map[string]string{}
var hSetMutex = sync.RWMutex{}

func hset(args []Value) Value {
	if len(args) != 3 {
		return Value{Typ: "error", Str: "ERR wrong number of arguments for 'hset' command"}
	}

	hash := args[0].Bulk
	key := args[1].Bulk
	value := args[2].Bulk

	hSetMutex.Lock()
	if _, ok := mapMap[hash]; !ok {
		mapMap[hash] = map[string]string{}
	}
	mapMap[hash][key] = value
	hSetMutex.Unlock()

	return Value{Typ: "string", Str: "OK"}
}

func hget(args []Value) Value {
	if len(args) != 2 {
		return Value{Typ: "error", Str: "ERR wrong number of arguments for 'hget' command"}
	}

	hash := args[0].Bulk
	key := args[1].Bulk

	hSetMutex.RLock()
	value, ok := mapMap[hash][key]
	hSetMutex.RUnlock()

	if !ok {
		return Value{Typ: "null"}
	}

	return Value{Typ: "bulk", Bulk: value}
}
*/

// func hgetall(args []string) string {
// 	if len(args) == 0 {
// 		return ""
// 	}
// 	return mapMap[args[0]]
// }