APPNAME = suuchi-store
VERSION=`git log -n1 --format="%h"`
BUILD_TIMESTAMP=`date`
TESTFLAGS=-v -cover -covermode=atomic -bench=.
TEST_COVERAGE_THRESHOLD=20.0
ifndef $(GOPATH)
	GOPATH=$(shell go env GOPATH)
	export GOPATH
endif

all: setup test-ci build

build:
	go build -tags netgo -ldflags "-w -s -X 'main.AppVersion=${VERSION}' -X 'main.BuildTimestamp=${BUILD_TIMESTAMP}'" -o ${APPNAME} .

setup:
	go mod download
	go install github.com/wadey/gocovmerge@latest

test-only:
	go test ${TESTFLAGS} github.com/ashwanthkumar/suuchi-store/${name}

test:
	go test ${TESTFLAGS} -coverprofile=main.txt github.com/ashwanthkumar/suuchi-store/
	go test ${TESTFLAGS} -coverprofile=store.txt github.com/ashwanthkumar/suuchi-store/hasher

test-ci: test
	${GOPATH}/bin/gocovmerge *.txt > coverage.txt
	@go tool cover -html=coverage.txt -o coverage.html
	@go tool cover -func=coverage.txt | grep "total:" | awk '{print $$3}' | sed -e 's/%//' > cov_total.out
	@bash -c 'COVERAGE=$$(cat cov_total.out);	\
		echo "Current Coverage % is $$COVERAGE, expected is ${TEST_COVERAGE_THRESHOLD}.";	\
		exit $$(echo $$COVERAGE"<${TEST_COVERAGE_THRESHOLD}" | bc -l)'
