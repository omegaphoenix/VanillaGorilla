package edu.caltech.nanodb.functions;


import java.util.HashSet;
import java.util.List;

import edu.caltech.nanodb.expressions.Expression;

import edu.caltech.nanodb.relations.ColumnType;
import edu.caltech.nanodb.relations.Schema;
import edu.caltech.nanodb.relations.SQLDataType;


/**
 * This aggregate function can be used to compute both <tt>COUNT(...)</tt> and
 * <tt>COUNT(DISTINCT ...)</tt> aggregate functions.  In addition, the
 * <tt>COUNT(DISTINCT ...)</tt> operation can consume either sorted or unsorted
 * values to compute the distinct count.
 */
public class CountAggregate extends AggregateFunction {

    /** Contains the current number of items */
    private int count;


    /**
     * Contains all of the values seen so far, used for COUNT DISTINCT to keep
     * track of distinct values
     */
    private HashSet<Object> valuesSeen = new HashSet<Object>();


    /** Stores the most recently seen object */
    private Object lastValueSeen;


    /** Boolean that is true if we are counting distinct values */
    private boolean distinct;


    /** 
     * True if the inputs are sorted. We use this to count distinct values
     * faster since if the inputs are sorted, we do not need to add them to
     * a set.
     */
    private boolean sortedInputs;


    public CountAggregate(boolean distinct, boolean sortedInputs) {
        super(/* supportsDistinct */ true);
        this.distinct = distinct;
        this.sortedInputs = sortedInputs;
    }


    @Override
    public void clearResult() {
        count = -1;

        if (distinct) {
            if (sortedInputs)
                lastValueSeen = null;
            else
                valuesSeen.clear();
        }
    }


    @Override
    public void addValue(Object value) {
        // NULL values are ignored by aggregate functions.
        if (value == null)
            return;

        if (count == -1)
            count = 0;

        // Counting distinct values requires more checking than just counting
        // any value that comes through.
        if (distinct) {
            if (sortedInputs) {
                // If the inputs are sorted then we increment the count every
                // time we see a new value.
                if (lastValueSeen == null || !lastValueSeen.equals(value)) {
                    lastValueSeen = value;
                    count++;
                }
            }
            else {
                // If the inputs are hashed then we increment the count every
                // time the value isn't already in the hash-set.
                if (valuesSeen.add(value))
                    count++;
            }
        }
        else {
            // Non-distinct count.  Just increment on any non-null value.
            count++;
        }
    }


    @Override
    public Object getResult() {
        // A value of -1 indicates a NULL result.
        return (count == -1 ? null : Integer.valueOf(count));
    }
    
    
    @Override
    public ColumnType getReturnType(List<Expression> args, Schema schema) {
        if (args.size() != 1) {
            throw new IllegalArgumentException(
                    "Count aggregate function takes 1 argument; got " +
                            args.size());
        }

        // When counting, the resulting aggregate column is always an integer
        return new ColumnType(SQLDataType.INTEGER);
    }
}
