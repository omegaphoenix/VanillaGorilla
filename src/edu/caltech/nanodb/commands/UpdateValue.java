package edu.caltech.nanodb.commands;


import edu.caltech.nanodb.expressions.*;


/**
 * This class represents a single column-name/expression pair in an
 * <tt>UPDATE</tt> statement's <tt>SET</tt> clause.
 */
public class UpdateValue {

    /**
     * The column that will receive the value when the update is applied to
     * a tuple.
     */
    private String columnName;


    /** The expression that will be computed and stored. */
    private Expression expression;


    /**
     * Construct an update-value object from a column name and an expression.
     *
     * @param colName The column name that will receive the value.
     * @param e The expression that generates the value to store.
     *
     * @throws java.lang.NullPointerException if <code>colName</code> or
     * <code>e</code> is <code>null</code>
     */
    public UpdateValue(String colName, Expression e) {
        if (colName == null || e == null)
            throw new NullPointerException();

        columnName = colName;
        expression = e;
    }


    public String toString() {
        return columnName + ":" + expression;
    }


    public String getColumnName() {
        return columnName;
    }


    /** Returns the expression for generating the update-value. */
    public Expression getExpression() {
        return expression;
    }
}

