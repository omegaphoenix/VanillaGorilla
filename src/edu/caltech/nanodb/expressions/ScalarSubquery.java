package edu.caltech.nanodb.expressions;


import java.io.IOException;

import edu.caltech.nanodb.queryast.SelectClause;
import edu.caltech.nanodb.relations.ColumnInfo;
import edu.caltech.nanodb.relations.Schema;
import edu.caltech.nanodb.relations.SchemaNameException;
import edu.caltech.nanodb.relations.Tuple;


/**
 * This class represents a scalar subquery embedded in another query's
 * predicate.
 */
public class ScalarSubquery extends SubqueryOperator {
    /** The cached result of evaluating the scalar subquery. */
    Object result;


    /**
     * A flag indicating whether the subquery has been evaluated, and
     * therefore whether {@link #result} is valid or not.
     */
    boolean evaluated;


    public ScalarSubquery(SelectClause subquery) {
        if (subquery == null)
            throw new IllegalArgumentException("subquery cannot be null");

        this.subquery = subquery;

        this.evaluated = false;
    }


    public ColumnInfo getColumnInfo(Schema schema) throws SchemaNameException {
        Schema subquerySchema = subquery.getSchema();

        int numCols = subquerySchema.numColumns();
        if (numCols != 1) {
            throw new ExpressionException(
                "Scalar subquery must produce exactly one column (got " +
                numCols + " instead)");
        }

        // Return whatever the subquery produces for its column.
        return subquerySchema.getColumnInfo(0);
    }


    public Object evaluate(Environment env) {
        if (subqueryPlan == null)
            throw new IllegalStateException("No execution plan for subquery");

        if (!evaluated) {
            // Initialize the subquery plan so that it can be evaluated.
            subqueryPlan.initialize();

            try {
                // Get the first tuple from the subquery, and make sure it has
                // exactly one row and one column.

                Tuple t1 = subqueryPlan.getNextTuple();
                if (t1 == null) {
                    throw new ExpressionException(
                        "Scalar subquery must produce exactly one row (got 0)");
                }

                int numCols = t1.getColumnCount();
                if (numCols != 1) {
                    t1.unpin();
                    throw new ExpressionException(
                        "Scalar subquery must produce exactly one column (got " +
                            numCols + " instead)");
                }

                // Make sure the subquery plan doesn't generate a second tuple!

                Tuple t2 = subqueryPlan.getNextTuple();
                if (t2 != null) {
                    t1.unpin();
                    t2.unpin();
                    throw new ExpressionException(
                        "Scalar subquery must produce exactly one row (got > 1)");
                }

                result = t1.getColumnValue(0);
                t1.unpin();
            }
            catch (IOException e) {
                throw new ExpressionException("Error while evaluating subquery", e);
            }
        }

        return result;
    }


    @Override
    public Expression traverse(ExpressionProcessor p) {
        p.enter(this);
        // We do not traverse the subquery; it is treated as a "black box"
        // by the expression-traversal mechanism.
        return p.leave(this);
    }


    @Override
    public String toString() {
        return "(" + subquery.toString() + ")";
    }


    /**
     * Checks if the argument is an expression with the same structure, but not
     * necessarily the same references.
     *
     * @design This function operates correctly on the assumption that all
     *         supported types override Object.equals().
     *
     * @param obj the object to which we are comparing
     */
    @Override
    public boolean equals(Object obj) {
        if (obj instanceof ScalarSubquery) {
            ScalarSubquery other = (ScalarSubquery) obj;
            return subquery.equals(other.subquery);
        }
        return false;
    }


    /**
     * Computes the hash-code of this scalar subquery expression.
     */
    @Override
    public int hashCode() {
        return subquery.hashCode();
    }


    /**
     * Creates a copy of expression.
     *
     * @design The reference of the internal value is simply copied since the
     *         value types are all immutable.
     */
    @Override
    protected Object clone() throws CloneNotSupportedException {
        // We don't clone SelectClause expressions at this point since they are
        // currently not cloneable.
        return super.clone();
    }
}

