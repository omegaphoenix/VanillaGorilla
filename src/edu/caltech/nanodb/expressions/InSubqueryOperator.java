package edu.caltech.nanodb.expressions;


import java.io.IOException;

import edu.caltech.nanodb.queryast.SelectClause;
import edu.caltech.nanodb.relations.ColumnInfo;
import edu.caltech.nanodb.relations.ColumnType;
import edu.caltech.nanodb.relations.Schema;
import edu.caltech.nanodb.relations.SchemaNameException;
import edu.caltech.nanodb.relations.Tuple;


/**
 * <p>
 * This class implements the <tt>expr IN (subquery)</tt> operator.  This
 * operation may be optimized out of a query, but if it is not, it can still
 * be evaluated although it will be slow.
 * </p>
 * <p>
 * The <tt>expr NOT IN (...)</tt> operator is translated into <tt>NOT (expr IN
 * (...))</tt> by the parser.
 * </p>
 */
public class InSubqueryOperator extends SubqueryOperator {
    /**
     * The expression to check against the set on the righthand side of the
     * <tt>IN</tt> operator.
     */
    Expression expr;


    public InSubqueryOperator(Expression expr, SelectClause subquery) {
        if (expr == null)
            throw new IllegalArgumentException("expr must be specified");

        if (subquery == null)
            throw new IllegalArgumentException("subquery must be specified");

        this.expr = expr;
        this.subquery = subquery;
    }


    public ColumnInfo getColumnInfo(Schema schema) throws SchemaNameException {
        // Comparisons always return Boolean values, so just pass a Boolean
        // value in to the TypeConverter to get out the corresponding SQL type.
        ColumnType colType =
            new ColumnType(TypeConverter.getSQLType(Boolean.FALSE));
        return new ColumnInfo(colType);
    }


    /**
     * Evaluates this comparison expression and returns either
     * {@link java.lang.Boolean#TRUE} or {@link java.lang.Boolean#FALSE}.  If
     * either the left-hand or right-hand expression evaluates to
     * <code>null</code> (representing the SQL <tt>NULL</tt> value), then the
     * expression's result is always <code>FALSE</code>.
     *
     * @design (Donnie) We have to suppress "unchecked operation" warnings on
     *         this code, since {@link Comparable} is a generic (and thus allows
     *         us to specify the type of object being compared), but we want to
     *         use it without specifying any types.
     */
    @SuppressWarnings("unchecked")
    public Object evaluate(Environment env) throws ExpressionException {
        Object exprObj = expr.evaluate(env);
        if (exprObj == null)
            return null;

        if (subqueryPlan == null)
            throw new IllegalStateException("No execution plan for subquery");

        try {
            subqueryPlan.initialize();
            while (true) {
                Tuple tup = subqueryPlan.getNextTuple();
                if (tup == null)
                    break;

                Object tupObj = tup.getColumnValue(0);
                tup.unpin();

                if (CompareOperator.areObjectsEqual(exprObj, tupObj))
                    return Boolean.TRUE;
            }
        }
        catch (IOException e) {
            throw new ExpressionException("Error while evaluating subquery", e);
        }

        // If we got here, nothing matched.
        return Boolean.FALSE;
    }


    @Override
    public Expression traverse(ExpressionProcessor p) {
        p.enter(this);

        expr = expr.traverse(p);

        // We do not traverse the subquery; it is treated as a "black box"
        // by the expression-traversal mechanism.

        return p.leave(this);
    }


    /**
     * Returns a string representation of this comparison expression and its
     * subexpressions.
     */
    @Override
    public String toString() {
        StringBuilder buf = new StringBuilder();

        // Convert all of the components into string representations.

        buf.append(expr.toString()).append(" IN (");
        buf.append(subquery.toString());
        buf.append(')');

        return buf.toString();
    }


    /**
     * Checks if the argument is an expression with the same structure, but not
     * necessarily the same references.
     *
     * @param obj the object to which we are comparing
     */
    @Override
    public boolean equals(Object obj) {

        if (obj instanceof InSubqueryOperator) {
            InSubqueryOperator other = (InSubqueryOperator) obj;
            return expr.equals(other.expr) && subquery.equals(other.subquery);
        }

        return false;
    }


    @Override
    public int hashCode() {
        int hash = 7;
        hash = 31 * hash + expr.hashCode();
        hash = 31 * hash + subquery.hashCode();
        return hash;
    }


    /**
     * Creates a copy of expression.  This method is used by the
     * {@link Expression#duplicate} method to make a deep copy of an expression
     * tree.
     */
    @Override
    @SuppressWarnings("unchecked")
    protected Object clone() throws CloneNotSupportedException {
        InSubqueryOperator op = (InSubqueryOperator) super.clone();

        // Clone the subexpressions.  Don't clone the subquery,
        // since subqueries currently aren't cloneable.
        op.expr = (Expression) expr.clone();

        return op;
    }
}
