package middleware

type Queue struct {
	Name string
}

func NewQueue(name string) Queue {
	return Queue{Name: name}
}
