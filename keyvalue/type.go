package keyvalue

type T struct {
	Key   string
	Value string
}

func KV(k, v string) T {
	return T{Key: k, Value: v}
}
