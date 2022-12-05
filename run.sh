#!/bin/sh

g++ stl_hash.cc -lpthread -std=c++11 -O2 -o stl_hash
g++ ska_hash.cc -lpthread -std=c++11 -O2 -o ska_hash

for nthr in 1 2 4 8 16 32; do
# for nthr in 32; do
  echo "stl hash thread num $nthr"
  ./stl_hash -t $nthr -e 1000000
  echo "ska hash thread num $nthr"
  ./ska_hash -t $nthr -e 1000000
done

