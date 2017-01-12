package edu.caltech.nanodb.functions;


import java.util.List;

import edu.caltech.nanodb.expressions.Expression;
import edu.caltech.nanodb.relations.ColumnType;
import edu.caltech.nanodb.relations.Schema;


/**
 * This aggregate function can be used to compute either the minimum or the
 * maximum of a collection of values.
 */
public class MinMaxAggregate extends AggregateFunction {

    /**
     * This value is set to 1 if the aggregate function is computing the minimum
     * value, or -1 if the aggregate function is computing the maximum value.
     * It is used to avoid having another conditional in the {@link #addValue}
     * implementation.
     */
    private int minimumSwitch;


    /**
     * The actual result of the aggregate function, or <tt>null</tt> if the
     * function hasn't been handed a non-<tt>NULL</tt> value yet.
     */
    private Comparable result;


    public MinMaxAggregate(boolean minimum) {
        super(/* supportsDistinct */ false);

        minimumSwitch = minimum ? 1 : -1;
        clearResult();
    }


    @Override
    public void clearResult() {
        result = null;
    }


    @Override
    @SuppressWarnings("unchecked")
    public void addValue(Object value) {
        // NULL values are ignored by aggregate functions.
        if (value == null)
            return;

        Comparable comparable = (Comparable) value;

        // compareTo() returns positive value if LHS > RHS.  If configured to do
        // minimum, if LHS > RHS then we want to replace LHS.  If configured to
        // do maximum, if LHS < RHS then the result will be negative, and we
        // flip the sign back to positive with the value of minimumSwitch.
        if (result == null || minimumSwitch * result.compareTo(comparable) > 0)
            result = comparable;
    }


    @Override
    public Object getResult() {
        return result;
    }


    @Override
    public ColumnType getReturnType(List<Expression> args, Schema schema) {
        if (args.size() != 1) {
            throw new IllegalArgumentException(
                "Min/max aggregate function takes 1 argument; got " +
                args.size());
        }

        // When finding the min or max, the resulting aggregate column is the
        // same type as the values of the column.
        return args.get(0).getColumnInfo(schema).getType();
    }
}
