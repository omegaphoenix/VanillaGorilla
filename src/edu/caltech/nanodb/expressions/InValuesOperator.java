package edu.caltech.nanodb.expressions;


import java.util.ArrayList;

import edu.caltech.nanodb.relations.ColumnInfo;
import edu.caltech.nanodb.relations.ColumnType;
import edu.caltech.nanodb.relations.Schema;
import edu.caltech.nanodb.relations.SchemaNameException;


/**
 * <p>
 * This class implements the <tt>expr IN (values)</tt> operator.  This
 * operation may be optimized out of a query, but if it is not, it can still
 * be evaluated although it will be slow.
 * </p>
 * <p>
 * The <tt>expr NOT IN (...)</tt> operator is translated into <tt>NOT (expr IN
 * (...))</tt> by the parser.
 * </p>
 */
public class InValuesOperator extends Expression {
    /**
     * The expression to check against the set on the righthand side of the
     * <tt>IN</tt> operator.
     */
    Expression expr;


    /**
     * If the righthand side of the <tt>IN</tt> operator is a list of values
     * (expressions, specifically), this is the list of values.
     */
    ArrayList<Expression> values;


    public InValuesOperator(Expression expr, ArrayList<Expression> values) {
        if (expr == null)
            throw new IllegalArgumentException("expr must be specified");

        if (values == null)
            throw new IllegalArgumentException("values must be specified");

        if (values.isEmpty())
            throw new IllegalArgumentException("values must be non-empty");

        this.expr = expr;
        this.values = values;
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

        for (Expression valueExpr : values) {
            Object valueObj = valueExpr.evaluate(env);
            if (valueObj == null)
                return null;

            if (CompareOperator.areObjectsEqual(exprObj, valueObj))
                return Boolean.TRUE;
        }

        // If we got here, nothing matched.
        return Boolean.FALSE;
    }


    @Override
    public Expression traverse(ExpressionProcessor p) {
        Expression r;

        p.enter(this);

        expr = expr.traverse(p);

        for (int i = 0; i < values.size(); i++) {
            r = values.get(i).traverse(p);
            values.set(i, r);
        }

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

        boolean first = true;
        for (Expression e : values) {
            if (first)
                first = false;
            else
                buf.append(", ");

            buf.append(e.toString());
        }

        buf.append(')');

        return buf.toString();
    }


    /**
     * If the <tt>IN</tt> operation has a list of values on the righthand side,
     * this will be the list of values.  Otherwise, this will be <tt>null</tt>.
     *
     * @return the list of values on the righthand side of the <tt>IN</tt>
     *         operation, or <tt>null</tt>.
     */
    public ArrayList<Expression> getValues() {
        return values;
    }


    /**
     * Checks if the argument is an expression with the same structure, but not
     * necessarily the same references.
     *
     * @param obj the object to which we are comparing
     */
    @Override
    public boolean equals(Object obj) {

        if (obj instanceof InValuesOperator) {
            InValuesOperator other = (InValuesOperator) obj;
            return expr.equals(other.expr) && values.equals(other.values);
        }

        return false;
    }


    @Override
    public int hashCode() {
        int hash = 7;
        hash = 31 * hash + expr.hashCode();
        hash = 31 * hash + values.hashCode();
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
        InValuesOperator op = (InValuesOperator) super.clone();

        // Clone the subexpressions.
        op.expr = (Expression) expr.clone();
        op.values = (ArrayList<Expression>) values.clone();

        return op;
    }
}
