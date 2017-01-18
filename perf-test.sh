#!/bin/bash
start=`date +%s.%N`
./nanodb < schemas/insert-perf/make-insperf.sql
./nanodb < schemas/insert-perf/ins100-del.sql > test.txt
echo "100 inserts and deletes" >> results.txt
tail -5 test.txt >> results.txt
ls -l datafiles/ >> results.txt

./nanodb < schemas/insert-perf/make-insperf.sql
./nanodb < schemas/insert-perf/ins20k.sql > test.txt
echo "\n20k inserts" >> results.txt
tail -5 test.txt >> results.txt
ls -l datafiles/ >> results.txt

./nanodb < schemas/insert-perf/make-insperf.sql
./nanodb < schemas/insert-perf/ins50k.sql > test.txt
echo "\n50k inserts" >> results.txt
tail -5 test.txt >> results.txt
ls -l datafiles/ >> results.txt

./nanodb < schemas/insert-perf/make-insperf.sql
./nanodb < schemas/insert-perf/ins50k-del.sql > test.txt
echo "\n50k inserts and deletes" >> results.txt
tail -5 test.txt >> results.txt
ls -l datafiles/ >> results.txt
end=`date +%s.%N`
runtime=$( echo "$end - $start" | bc -l )
echo "Runtime was $runtime\n" >> results.txt
