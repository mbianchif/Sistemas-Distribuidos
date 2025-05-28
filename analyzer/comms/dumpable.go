package comms

type Dumpable interface {
	Dump(int) error
}
