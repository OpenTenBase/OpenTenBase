#! /bin/sh
# src/test/oracle_test/test.sh


DIRECTORY='sql'
DIRTMP='tmp'
DIREXCEPTED='expected'

rm -rf "$DIRTMP"/*

# connect to psql with following params
TESTDB='testdb'
HOST='localhost'
PORT=30004
USER='opentenbase'

for sqlfile in $DIRECTORY/*.sql; do
  echo "Executing $sqlfile..."
  psql -d $TESTDB -h $HOST -p $PORT -U $USER -f "$sqlfile" > "$DIRTMP/$(basename "$sqlfile").out"
done


if diff -r -q $DIREXCEPTED $DIRTMP > /dev/null; then
  echo "pass"
else
  echo "error"
fi

rm -rf "$DIRTMP"/*

