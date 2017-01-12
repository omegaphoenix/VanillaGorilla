package edu.caltech.nanodb.expressions;

import edu.caltech.nanodb.queryast.SelectClause;
import edu.caltech.nanodb.plannodes.PlanNode;

/**
 * <p>
 * This class is the superclass of all expressions that can hold subqueries,
 * such as the <tt>IN</tt> operator, the <tt>EXISTS</tt> operator, and scalar
 * subqueries.
 * </p>
 * <p>
 * Subqueries are different from other kinds of expressions in that they
 * require an execution plan to evaluate the subquery expression node.  Thus,
 * evaluating the subquery expression requires a recursive invocation of the
 * query evaluator.  In general, this recursive invocation approach will be
 * slow for larger queries, and a good planner/optimizer will replace subquery
 * operators with joins, semijoins, etc. wherever it can.
 * </p>
 * <p>
 * <b>Note that all subqueries are considered "black boxes" to the
 * expression-traversal mechanism.</b>  That is, traversing an expression
 * that contains a subquery will not descend into the subquery's expressions.
 * The reason for this is simple - the subquery may include column-references
 * to schemas that only make sense in the context of the subquery; thus,
 * the subquery should be processed separately from the expression traversal.
 * </p>
 *
 * @see edu.caltech.nanodb.queryast.SubquerySchemaComputer
 */
public abstract class SubqueryOperator extends Expression {

    /**
     * This is the parsed representation of the subquery that the operator
     * uses for its operation.
     */
    protected SelectClause subquery;


    /**
     * The execution plan for the subquery that is used by the operator.  This
     * will be {@code null} until the subquery has actually been analyzed and
     * planned by the query planner.
     */
    protected PlanNode subqueryPlan;


    /**
     * Returns the parsed representation of the subquery that is used by this
     * operator.
     *
     * @return the parsed representation of the subquery that is used by this
     *         operator.
     */
    public SelectClause getSubquery() {
        return subquery;
    }


    /**
     * Sets the execution plan for evaluating the subquery.
     *
     * @param plan the execution plan for evaluating the subquery.
     */
    public void setSubqueryPlan(PlanNode plan) {
        subqueryPlan = plan;
    }


    /**
     * Retrieves the current execution plan for the subquery.
     *
     * @return the current execution plan for the subquery.
     */
    public PlanNode getSubqueryPlan() {
        return subqueryPlan;
    }
}
