# Inspired by https://github.com/prometheus/node_exporter/blob/fc3931c924511531fe252342489af9468394f2f1/Makefile and https://github.com/prometheus/node_exporter/blob/master/Makefile.common

DOCKER_IMAGE ?= prometheus-vpc-flow-metrics
REVISION ?= $(shell git rev-parse --short HEAD 2> /dev/null || echo 'unknown')
BRANCH   ?= $(shell git rev-parse --abbrev-ref HEAD 2> /dev/null || echo 'unknown')

VERSION  ?= $(shell yq '.appVersion' chart/prometheus-vpc-flow-metrics/Chart.yaml 2> /dev/null || echo 'unknown')
BUILDTIME := $(shell date -Iseconds)

LDFLAGS   := -X main.Version=$(VERSION)
LDFLAGS   += -X main.Revision=$(REVISION)
LDFLAGS   += -X main.Branch=$(BRANCH)
LDFLAGS   += -X main.BuildTime=$(BUILDTIME)

GOFLAGS   := -ldflags "$(LDFLAGS)"

run:
	go run $(GOFLAGS) cmd/main.go

build:
	go build $(GOFLAGS) cmd/main.go

vet:
	go vet cmd/
