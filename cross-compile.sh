#!/bin/bash

GOOS=darwin GOARCH=amd64 go build -o committed-darwin-amd64
file committed-darwin-amd64
GOOS=linux GOARCH=amd64 go build -o committed-linux-amd64
file committed-linux-amd64
GOOS=windows GOARCH=amd64 go build -o committed-windows-amd64.exe
file committed-windows-amd64.exe