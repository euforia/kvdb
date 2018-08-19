package kvdb

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

var testOpenNegCases = []string{
	"",
	"/a/b/c",
	"a",
}

func Test_Open(t *testing.T) {
	for _, c := range testOpenNegCases {
		_, err := Open(c)
		assert.NotNil(t, err)
	}
}
