package edu.caltech.nanodb.expressions;


/**
 * This class represents an expression that results are ordered by, as well as
 * whether the order is ascending or descending.  Lists of this object specify
 * ordering of results from a query, as well as the ordering of tuples in a
 * sequential file, or attributes in an ordered index.
 */
public class OrderByExpression {

    /** The expression that the results should be ordered by. */
    private Expression expression;

    /**
     * A flag indicating whether the results should be in ascending or
     * descending order.  A value of <tt>true</tt> means ascending order.
     */
    private boolean ascending;


    /**
     * Initialize a new order-by object with the specified expression.  Results
     * will be in ascending order.
     *
     * @param expression The expression to order results by.
     */
    public OrderByExpression(Expression expression) {
        this(expression, true);
    }


    /**
     * Initialize a new order-by object with the specified expression and order.
     *
     * @param expression The expression to order results by.
     *
     * @param ascending a value of <tt>true</tt> will cause the results to be
     *        in ascending order; a value of <tt>false</tt> will cause them to
     *        be in descending order
     */
    public OrderByExpression(Expression expression, boolean ascending) {
        this.expression = expression;
        this.ascending = ascending;
    }


    /**
     * Returns the expression that results will be sorted on
     *
     * @return the expression that results will be sorted on
     */
    public Expression getExpression() {
        return expression;
    }


    /**
     * Returns the flag indicating whether the results should be in ascending or
     * descending order.
     *
     * @return <tt>true</tt> if the results should be in ascending order,
     *         <tt>false</tt> otherwise.
     */
    public boolean isAscending() {
        return ascending;
    }


    @Override
    public String toString() {
        return expression.toString() + ' ' + (ascending ? "ASC" : "DESC");
    }
}
