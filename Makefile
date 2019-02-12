VERSION ?= latest

.PHONY: cover
cover:
	@go test -coverpkg=./... -coverprofile=coverage.txt ./...