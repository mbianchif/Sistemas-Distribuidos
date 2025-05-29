package middleware

type Dumpable interface {
	Dump(int) error
}
