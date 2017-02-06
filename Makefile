

GOBUILD = go build
GOTEST = go test
GOGET = go get -u

VARS = vars.mk
$(shell ./build_config ${VARS})
include ${VARS}

.PHONY: main clean test

main:
	${GOBUILD} -o bin/persistent src/run_persistent.go	

deps:
	${GOGET} github.com/brg-liuwei/gotools
	${GOGET} github.com/dongjiahong/gotools
	${GOGET} github.com/go-sql-driver/mysql

test:
	./auto_test.sh

clean:
	@rm bin/persistent
