package edu.caltech.nanodb.expressions;


import java.io.IOException;

import edu.caltech.nanodb.plannodes.PlanNode;
import edu.caltech.nanodb.queryast.SelectClause;
import edu.caltech.nanodb.relations.ColumnInfo;
import edu.caltech.nanodb.relations.ColumnType;
import edu.caltech.nanodb.relations.Schema;
import edu.caltech.nanodb.relations.SchemaNameException;
import edu.caltech.nanodb.relations.Tuple;


/**
 * <p>
 * This class implements the <tt>EXISTS (subquery)</tt> operator.  This
 * operation may be optimized out of a query, but if it is not, it can still be
 * evaluated although it will be slow.
 * </p>
 * <p>
 * The <tt>NOT EXISTS (subquery)</tt> clause is translated into
 * <tt>NOT (EXISTS (subquery))</tt> by the parser, as expected.
 * </p>
 */
public class ExistsOperator extends SubqueryOperator {

    public ExistsOperator(SelectClause subquery) {
        if (subquery == null)
            throw new IllegalArgumentException("subquery must be specified");

        this.subquery = subquery;
    }


    public ColumnInfo getColumnInfo(Schema schema) throws SchemaNameException {
        // Comparisons always return Boolean values, so just pass a Boolean
        // value in to the TypeConverter to get out the corresponding SQL type.
        ColumnType colType =
            new ColumnType(TypeConverter.getSQLType(Boolean.FALSE));
        return new ColumnInfo(colType);
    }


    public Object evaluate(Environment env) throws ExpressionException {
        if (subqueryPlan == null)
            throw new IllegalStateException("No execution plan for subquery");

        try {
            subqueryPlan.initialize();

            // See if the subquery will produce any tuples!
            Tuple tuple = subqueryPlan.getNextTuple();
            if (tuple != null)
                tuple.unpin();

            subqueryPlan.cleanUp();

            return Boolean.valueOf(tuple != null);
        }
        catch (IOException e) {
            throw new ExpressionException("Error while evaluating subquery", e);
        }
    }


    @Override
    public Expression traverse(ExpressionProcessor p) {
        p.enter(this);
        // We do not traverse the subquery; it is treated as a "black box"
        // by the expression-traversal mechanism.
        return p.leave(this);
    }


    /**
     * Returns a string representation of this <tt>EXISTS</tt> expression.
     */
    @Override
    public String toString() {
        StringBuilder buf = new StringBuilder();

        buf.append("EXISTS (");

        buf.append("query=").append(subquery);

        if (subqueryPlan != null) {
            String s = PlanNode.printNodeTreeToString(subqueryPlan);
            buf.append(", plan=").append(s);
        }

        buf.append(")");

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
        if (obj instanceof ExistsOperator) {
            ExistsOperator other = (ExistsOperator) obj;
            return subquery.equals(other.subquery);
        }
        return false;
    }


    @Override
    public int hashCode() {
        return subquery.hashCode();
    }


    /**
     * Creates a copy of expression.  This method is used by the
     * {@link Expression#duplicate} method to make a deep copy of an expression
     * tree.
     */
    @Override
    protected Object clone() throws CloneNotSupportedException {
        ExistsOperator op = (ExistsOperator) super.clone();

        // Don't clone the subquery since subqueries currently aren't cloneable.
        return op;
    }
}
