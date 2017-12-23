#!/bin/bash

tests=$(grep 'func Test' test_test.go | grep -v Snapshot | sed 's/(.*//g;s/func //')

for test in $tests; do
  go test --run ${test}$
done 2>&1 | tee out.log
