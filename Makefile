.PHONY: test
test:
	sh -c "go test -cover -tags=test $(shell go list ./...)"