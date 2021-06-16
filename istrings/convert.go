package istrings

func Slice(x []string) []IString {
	l := make([]IString, len(x))
	for i := range x {
		l[i] = Get(x[i])
	}
	return l
}

func SliceAndErr(x []string, err error) ([]IString, error) {
	return Slice(x), err
}
