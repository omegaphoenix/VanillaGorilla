package edu.caltech.nanodb.queryeval;

import java.io.IOException;
import java.util.List;

import edu.caltech.nanodb.expressions.SubqueryOperator;
import edu.caltech.nanodb.plannodes.PlanNode;
import edu.caltech.nanodb.queryast.SelectClause;
import edu.caltech.nanodb.queryast.SelectValue;

/**
 * This class is used by the overall planner to handle sub-queries.
 */
public class SubqueryPlanner {

    /**
     * Planner for creating sub-query plans.
     */
    private CostBasedJoinPlanner planner;

    /**
     * Constructs the SubqueryPlanner.
     *
     * @param planner this constructs plans out of plannodes.
     */
    public SubqueryPlanner(CostBasedJoinPlanner planner) {
        this.planner = planner;
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
        query.setSubqueryPlan(plan);
    }

    /**
     * Processes all sub-queries in a query.
     *
     * @param selClause
     * @param enclosingSelects
     * @throws IOException
     */
    public void processSubqueries(SelectClause selClause,
                                  List<SelectClause> enclosingSelects) throws IOException {

        /**
         * TODO: Refactor code to use/implement this function, or remove it, depending on
         * how we want to structure our code.
         */
        List<SelectValue> selectValues = selClause.getSelectValues();
    }

}
