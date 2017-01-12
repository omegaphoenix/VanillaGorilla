package edu.caltech.test.nanodb.expressions;


import java.util.SortedMap;

import org.testng.annotations.*;

import edu.caltech.nanodb.expressions.ColumnName;
import edu.caltech.nanodb.expressions.TupleLiteral;

import edu.caltech.nanodb.relations.ColumnInfo;
import edu.caltech.nanodb.relations.ColumnType;
import edu.caltech.nanodb.relations.SQLDataType;


/**
 * This test class exercises the functionality of the
 * {@link edu.caltech.nanodb.expressions.TupleLiteral} class.
 **/
@Test
public class TestTupleLiteral {

    public void testSimpleCtors() {
        TupleLiteral tuple;

        tuple = new TupleLiteral();
        assert tuple.getColumnCount() == 0;

        tuple = new TupleLiteral(5);
        assert tuple.getColumnCount() == 5;
        for (int i = 0; i < 5; i++) {
            assert tuple.getColumnValue(i) == null;
            assert tuple.isNullValue(i);
        }
    }


    /** This test exercises the <code>addValue()</code> methods. */
    public void testAddValues() {
        TupleLiteral tuple = new TupleLiteral();

        tuple.addValue(new Integer(3));
        tuple.addValue("hello");
        tuple.addValue(new Double(2.1));
        tuple.addValue(null);
        tuple.addValue(new Long(-6L));

        assert tuple.getColumnCount() == 5;

        assert !tuple.isNullValue(0);
        assert !tuple.isNullValue(1);
        assert !tuple.isNullValue(2);
        assert  tuple.isNullValue(3);
        assert !tuple.isNullValue(4);

        assert tuple.getColumnValue(0).equals(new Integer(3));
        assert tuple.getColumnValue(1).equals("hello");
        assert tuple.getColumnValue(2).equals(new Double(2.1));
        assert tuple.getColumnValue(3) == null;
        assert tuple.getColumnValue(4).equals(new Long(-6L));
    }


    /** This test exercises the constructor that duplicates a tuple. */
    public void testTupleCtor() {
        TupleLiteral tuple1 = new TupleLiteral();
        tuple1.addValue(new Integer(5));
        tuple1.addValue(null);
        tuple1.addValue("hello");

        TupleLiteral tuple2 = new TupleLiteral(tuple1);
        assert(tuple2.getColumnCount() == 3);

        assert(tuple2.getColumnValue(0).equals(new Integer(5)));

        assert(tuple2.isNullValue(1));
        assert(tuple2.getColumnValue(1) == null);

        assert(tuple2.getColumnValue(2).equals("hello"));
    }


    /** This test exercises the <tt>appendTuple()</tt> method. */
    public void testAppendTuple() {
        TupleLiteral tuple1 = new TupleLiteral();
        tuple1.addValue(new Integer(5));
        tuple1.addValue(null);
        tuple1.addValue("hello");

        TupleLiteral tuple2 = new TupleLiteral();
        tuple2.appendTuple(tuple1);

        assert(tuple2.getColumnCount() == 3);

        assert(tuple2.getColumnValue(0).equals(new Integer(5)));

        assert(tuple2.isNullValue(1));
        assert(tuple2.getColumnValue(1) == null);

        assert(tuple2.getColumnValue(2).equals("hello"));
    }
}

