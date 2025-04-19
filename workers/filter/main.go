package main

import (
	"fmt"
	"workers/filter/config"
	impl "workers/filter/impl"
)

func main() {
	con, err := config.Create()
	if err != nil {
		panic(err)
	}

	w, err := impl.New(con)
	if err != nil {
		panic(err)
	}

	if err := w.Run(con); err != nil {
		panic(err)
	}

	fmt.Println("todo ok")
}
