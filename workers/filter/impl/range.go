package impl

import (
	"fmt"
	"strconv"
	"strings"
)

type Range struct {
	From     *int
	To       *int
	FromIncl bool
	ToIncl   bool
}

func parseMathRange(input string) (*Range, error) {
	if len(input) < 5 {
		return nil, fmt.Errorf("invalid range format: %s", input)
	}

	fromIncl := input[0] == '['
	toIncl := input[len(input)-1] == ']'

	body := input[1 : len(input)-1]
	parts := strings.Split(body, ",")
	if len(parts) != 2 {
		return nil, fmt.Errorf("invalid range body: %s", body)
	}

	var fromPtr, toPtr *int

	if parts[0] != "" {
		from, err := strconv.Atoi(parts[0])
		if err != nil {
			return nil, fmt.Errorf("invalid lower bound: %s", parts[0])
		}
		fromPtr = &from
	}

	if parts[1] != "" {
		to, err := strconv.Atoi(parts[1])
		if err != nil {
			return nil, fmt.Errorf("invalid upper bound: %s", parts[1])
		}
		toPtr = &to
	}

	return &Range{
		From:     fromPtr,
		To:       toPtr,
		FromIncl: fromIncl,
		ToIncl:   toIncl,
	}, nil
}

func (r *Range) Contains(value int) bool {
	if r.From != nil {
		if r.FromIncl {
			if value < *r.From {
				return false
			}
		} else {
			if value <= *r.From {
				return false
			}
		}
	}

	if r.To != nil {
		if r.ToIncl {
			if value > *r.To {
				return false
			}
		} else {
			if value >= *r.To {
				return false
			}
		}
	}

	return true
}
