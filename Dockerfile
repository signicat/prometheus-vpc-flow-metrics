FROM golang:1.26-alpine AS build

ARG LDFLAGS

WORKDIR /app

COPY . .

RUN go build -o prometheus-vpc-flow-metrics -ldflags "$LDFLAGS" cmd/main.go 

FROM alpine

WORKDIR /app

COPY --from=build /app/prometheus-vpc-flow-metrics prometheus-vpc-flow-metrics

CMD ["/app/prometheus-vpc-flow-metrics"]