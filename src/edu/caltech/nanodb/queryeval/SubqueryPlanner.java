package edu.caltech.nanodb.queryeval;

import java.io.IOException;
import java.util.List;

import edu.caltech.nanodb.expressions.Expression;
import edu.caltech.nanodb.expressions.ExpressionProcessor;
import edu.caltech.nanodb.expressions.SubqueryOperator;
import edu.caltech.nanodb.expressions.Environment;
import edu.caltech.nanodb.plannodes.PlanNode;
import edu.caltech.nanodb.queryast.SelectClause;
import edu.caltech.nanodb.queryast.SelectValue;

/**
 * This class is used by the overall planner to handle sub-queries.
 */
public class SubqueryPlanner implements ExpressionProcessor {

    /**
     * Planner for creating sub-query plans.
     */
    private CostBasedJoinPlanner planner;

    /**
     * Environment of parent plan.
     */
    private Environment env;


    /**
     * Constructs the SubqueryPlanner.
     *
     * @param planner this constructs plans out of plannodes.
     */
    public SubqueryPlanner(CostBasedJoinPlanner planner) {
        this.planner = planner;
        env = new Environment();
    }

    /**
     * ExpressionProcessor enter()
     * @param node the {@code Expression} node being entered
     */
    public void enter(Expression node) {
        if (node instanceof SubqueryOperator) {
            try {
                planSubquery((SubqueryOperator) node, null);
            }
            catch (IOException e) {
            }
        }
    }

    /**
     * ExpressionProcess leave()
     * @param node the {@code Expression} node being left
     *
     * @return leave node unchanged.
     */
    public Expression leave(Expression node) {
        return node;
    }

    /**
     * Creates a query plan for a subquery, and sets it for the SubqueryOperator.
     *
     * @param query the subquery to plan.
     * @throws IOException
     */
    public void planSubquery(SubqueryOperator query,
                             List<SelectClause> enclosingSelects) throws IOException {
        SelectClause select = query.getSubquery();
        PlanNode plan = planner.makePlan(select, enclosingSelects);
        plan.addParentEnvironmentToPlanTree(env);
        query.setSubqueryPlan(plan);
    }

    /**
     *  Accessor function to return environment.
     *  @return Environment
     */
    public Environment getEnvironment() {
        return env;
    }
}
