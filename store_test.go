package kvdb

import (
	"log"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

var testOpenNegCases = []string{
	"",
	"/a/b/c",
	"a",
}

func Test_Open(t *testing.T) {
	logger := log.New(os.Stderr, "", log.LstdFlags|log.Lmicroseconds)
	for _, c := range testOpenNegCases {
		_, err := Open(c, logger)
		assert.NotNil(t, err)
	}
}
