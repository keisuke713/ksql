package bufferpool_test

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestKsql(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "KSql Suite")
	defer GinkgoRecover()
}
