package edu.caltech.nanodb.functions;


import java.util.HashSet;
import java.util.List;

import edu.caltech.nanodb.expressions.ArithmeticOperator;
import edu.caltech.nanodb.expressions.Expression;

import edu.caltech.nanodb.relations.ColumnType;
import edu.caltech.nanodb.relations.Schema;


/**
 * This aggregate function can be used to compute both SUM and AVERAGE
 * functions. It computes both the sum and average of a collection of values.
 */
public class SumAvgAggregate extends AggregateFunction {

    /**
     * This value is set to true if we want to compute the average value.
     * Otherwise, we compute the sum.
     */
    private boolean computeAverage;


    /** Contains the actual value of the sum */
    private Object sum;


    /** The current count of items that have been added */
    private int count;


    /** Indicates whether we want distinct values or not */
    private boolean distinct;


    /** Set to keep track of distinct values */
    HashSet<Object> set;


    public SumAvgAggregate(boolean computeAverage, boolean distinct) {
        super(/* supportsDistinct */ true);
        this.computeAverage = computeAverage;
        this.distinct = distinct;
        if (distinct)
            set = new HashSet<Object>();
    }


    @Override
    public void clearResult() {
        sum = null;
        count = 0;
        if (distinct)
            set.clear();
    }


    @Override
    public void addValue(Object value) {
        if (value == null)
            return;

        // If we are doing distinct, then only add the values if it has not
        // yet appeared
        if (distinct && set.contains(value))
            return;
        else if (distinct)
            set.add(value);

        if (sum == null) {
            // This is the first value.  Store it.
            sum = value;
        }
        else {
            // Add in the new value.
            sum = ArithmeticOperator.evalObjects(ArithmeticOperator.Type.ADD,
                sum, value);
        }

        if (computeAverage)
            count++;
    }


    @Override
    public Object getResult() {
        if (sum == null) {
            return null;
        }
        else if (computeAverage) {
            // Compute average from the sum and count.
            return ArithmeticOperator.evalObjects(
                ArithmeticOperator.Type.DIVIDE, sum, Integer.valueOf(count));
        }
        else {
            // Just return the sum.
            return sum;
        }
    }


    @Override
    public ColumnType getReturnType(List<Expression> args, Schema schema) {
        if (args.size() != 1) {
            throw new IllegalArgumentException(
                "Sum/average aggregate function takes 1 argument; got " +
                args.size());
        }

        // When finding the min or max, the resulting aggregate column is the
        // same type as the values of the column.
        return args.get(0).getColumnInfo(schema).getType();
    }
}
