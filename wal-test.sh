#!/bin/bash
RECORDA="| 1 | abc    |  1.2 |"
RECORDA_ALT="| 1 | abc         |  1.2 |"
RECORDB="| 2 | defghi | -3.6 |"
RECORDB_ALT="| 2 | defghi      | -3.6 |"
RECORDC="| 3 | jklmnopqrst |  5.5 |"
RECORDC_ALT="| 3 | jklmnopqrst |  5.5 |"
OUTPUTFILE="output.temp"
ant compile
echo "DELETING datafiles DIRECTORY"
rm -R datafiles
./nanodb < schemas/writeahead/test1a.sql > $OUTPUTFILE
if grep -Fq "$RECORDA" "$OUTPUTFILE"
then
    echo "PASS"
else
    echo "FAIL - Test1a Missing Record $RECORDA"
fi
if grep -Fq "$RECORDB" "$OUTPUTFILE"
then
    echo "PASS"
else
    echo "FAIL - Test1a Missing Record $RECORDB"
fi
if grep -Fq "$RECORDC" "$OUTPUTFILE"
then
    echo "FAIL - Test1a Extra Record $RECORDC"
else
    echo "PASS"
fi
echo "RESTARTING NANODB"
./nanodb < schemas/writeahead/test1b.sql > $OUTPUTFILE
if grep -Fq "$RECORDA" "$OUTPUTFILE"
then
    echo "PASS"
else
    echo "FAIL - Test1b Missing Record $RECORDA"
fi
if grep -Fq "$RECORDB" "$OUTPUTFILE"
then
    echo "PASS"
else
    echo "FAIL - Test1b Missing Record $RECORDB"
fi
if grep -Fq "$RECORDC" "$OUTPUTFILE"
then
    echo "PASS"
else
    echo "FAIL - Test1b Missing Record $RECORDC"
fi
echo "RESTARTING NANODB"
./nanodb < schemas/writeahead/test1c.sql > $OUTPUTFILE
if grep -Fq "$RECORDA_ALT" "$OUTPUTFILE"
then
    echo "PASS"
else
    echo "FAIL - Test1c Missing Record $RECORDA"
fi
if grep -Fq "$RECORDB_ALT" "$OUTPUTFILE"
then
    echo "PASS"
else
    echo "FAIL - Test1c Missing Record $RECORDB"
fi
if grep -Fq "$RECORDC_ALT" "$OUTPUTFILE"
then
    echo "PASS"
else
    echo "FAIL - Test1c Extra Record $RECORDC"
fi
rm $OUTPUTFILE
