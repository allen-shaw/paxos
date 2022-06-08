#!/usr/bin/env zsh

echo "build paxoskv server"
go build -o paxoskv ./server/*.go
