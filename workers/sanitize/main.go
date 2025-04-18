package main

import "workers/sanitize/config"

func main() {
	con, err := config.Create()
	if err != nil {
		return
	}

	w, err := New(con)
	if err != nil {
		return
	}

	w.Run()
}
