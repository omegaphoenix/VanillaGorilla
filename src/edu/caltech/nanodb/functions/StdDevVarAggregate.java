package edu.caltech.nanodb.functions;


import java.util.ArrayList;
import java.util.List;

import edu.caltech.nanodb.expressions.ArithmeticOperator;
import edu.caltech.nanodb.expressions.Expression;

import edu.caltech.nanodb.relations.ColumnType;
import edu.caltech.nanodb.relations.Schema;


/**
 * This aggregate function can be used to compute either the standard deviation
 * or the variance of a collection of values.
 */
public class StdDevVarAggregate extends AggregateFunction {

    private boolean computeStdDev;

    private Object sum;

    private ArrayList<Object> values;

    public StdDevVarAggregate(boolean computeStdDev) {
        super(/* supportsDistinct */ false);
        this.computeStdDev = computeStdDev;
    }


    @Override
    public void clearResult() {
        sum = null;
        values = null;
    }


    @Override
    public void addValue(Object value) {
        if (value == null)
            return;

        if (values == null) {
            // This is the first value. Create a new array list and store it.
            values = new ArrayList<Object>();
            values.add(value);
        } else {
            // Store the new value
            values.add(value);
        }
                
        if (sum == null) {
            // This is the first value.  Store it.
            sum = value;
        }
        else {
            // Add in the new value.
            sum = ArithmeticOperator.evalObjects(ArithmeticOperator.Type.ADD,
                sum, value);
        }
    }

    
    @Override
    public Object getResult() {
        if (sum == null || values == null)
            return null;
        else {
            int count = values.size();
            // Compute average from the sum and count.
            Object avg = ArithmeticOperator.evalObjects(
                ArithmeticOperator.Type.DIVIDE, sum, Integer.valueOf(count));
            
            // Compute the sum of the square of the residuals.
            Object sumSquaresResids = squareDifference(values.get(0), avg);
            for (int i = 1; i < count; i++) {
                sumSquaresResids = ArithmeticOperator.evalObjects(
                    ArithmeticOperator.Type.ADD, sumSquaresResids,
                    squareDifference(values.get(i), avg));
            }
            
            // Compute the variance.
            Object var = ArithmeticOperator.evalObjects(
                ArithmeticOperator.Type.DIVIDE, sumSquaresResids, 
                Integer.valueOf(count));
            
            // Compute standard deviation if necessary.
            if (computeStdDev) {
                return ArithmeticOperator.evalObjects(
                    ArithmeticOperator.Type.POWER, var, Double.valueOf(.5));
            }
            else {
                return var;
            }
        }
    }
    
    
    @Override
    public ColumnType getReturnType(List<Expression> args, Schema schema) {
        if (args.size() != 1) {
            throw new IllegalArgumentException(
                "Stddev/variance aggregate function takes 1 argument; got " +
                args.size());
        }

        // When finding the min or max, the resulting aggregate column is the
        // same type as the values of the column.
        return args.get(0).getColumnInfo(schema).getType();
    }
    
 
    /** 
     * Helper function that computes the square of the difference between
     * two values. 
     */
    private Object squareDifference(Object value, Object avg) {
        return ArithmeticOperator.evalObjects(ArithmeticOperator.Type.POWER,
            ArithmeticOperator.evalObjects(ArithmeticOperator.Type.SUBTRACT, value, avg),
            Integer.valueOf(2));
    }
}
