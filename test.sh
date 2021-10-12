#!/bin/bash

## Test 1 (with Porcupine framework)
cd tester/test
go build

rm -rf logs
cp -r ../../logs .

./test logs 1

## Test 2 (with Knossos framework)
cd ../Knossos_testing
./test.sh