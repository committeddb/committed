FROM golang:1.12.7-alpine3.10
RUN apk add --update git make gcc g++ bash

ENV APPLICATION /go/committed

RUN mkdir -p ${APPLICATION}
ADD . ${APPLICATION}
WORKDIR ${APPLICATION}

RUN go build -o committed .

CMD ${APPLICATION}/committed