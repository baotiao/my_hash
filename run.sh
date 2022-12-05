#!/bin/sh

g++ stl_hash.cc -lpthread -std=c++11 -o stl_hash
g++ ska_hash.cc -lpthread -std=c++11 -o ska_hash

./stl_hash -t 1 -e 100000
./ska_hash -t 1 -e 100000

