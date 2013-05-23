#!/bin/bash

die () {
    echo >&2 "$@"
    exit 1
}

[ "$#" -eq 1 ] || die "1 argument required, $# provided"
echo $1 | grep -E -q '^[0-9]+$' || die "Numeric argument required, $1 provided"

echo "Running tests $1 times"

for i in `seq 1 $1`;
do
  ant test | grep "Failures: [^0], Errors: [^0]"
  echo "Finished test $i"
  ant systemtest | grep "Failures: [^0], Errors: [^0]"
  echo "Finished systemtest $i"
done

