package impl

import (
	"fmt"
	"strconv"
	"strings"
)

type Range struct {
	From *int
	To   *int
}

func parseMathRange(input string) (*Range, error) {
	parts := strings.Split(input, ",")
	if len(parts) != 2 {
		return nil, fmt.Errorf("the range must be defined with two parts, given %v", len(parts))
	}

	var from, to *int
	if len(parts[0]) > 0 {
		fromParsed, err := strconv.Atoi(parts[0])
		if err != nil {
			return nil, fmt.Errorf("the lower bound of the range is not a number")
		}
		from = &fromParsed
	}

	if len(parts[1]) > 0 {
		toParsed, err := strconv.Atoi(parts[1])
		if err != nil {
			return nil, fmt.Errorf("the upper bound of the range is not a numer")
		}
		to = &toParsed
	}

	return &Range{
		From: from,
		To:   to,
	}, nil
}

func (r *Range) Contains(value int) bool {
	if r.From != nil && value < *r.From {
		return false
	}

	if r.To != nil && *r.To <= value {
		return false
	}

	return true
}
