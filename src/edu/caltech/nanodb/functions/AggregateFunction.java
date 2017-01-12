package edu.caltech.nanodb.functions;


/**
 * This class provides the general abstraction for aggregate functions.
 */
public abstract class AggregateFunction extends ScalarFunction {

    /**
     * If this flag is true, the aggregate function supports the
     * <tt>DISTINCT</tt> keyword before its first argument.
     */
    private boolean supportsDistinct;


    public AggregateFunction(boolean supportsDistinct) {
        this.supportsDistinct = supportsDistinct;
    }


    public boolean supportsDistinct() {
        return supportsDistinct;
    }


    /**
     * Clears the aggregate function's current state so that the object can be
     * reused to compute an aggregate on another set of input values.
     */
    public abstract void clearResult();


    /**
     * Adds a value to the aggregate function so that it can update its
     * internal state.  Generally, aggregate functions ignore {@code null}
     * inputs (which represent SQL <tt>NULL</tt> values) when computing their
     * results.
     *
     * @param value the value to add to the aggregate function
     */
    public abstract void addValue(Object value);


    /**
     * Returns the aggregated result computed for this aggregate function.
     * Generally, if aggregate functions receive no non-<tt>null</tt> inputs
     * then they should produce a <tt>null</tt> result.  (<tt>COUNT</tt> is an
     * exception to this rule, producing 0 in that case.)
     *
     * @return the result of the aggregate computation.
     */
    public abstract Object getResult();
}
