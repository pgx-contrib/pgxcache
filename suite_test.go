package pgxcache_test

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestPgxcache(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "pgxcache Suite")
}
