package edu.caltech.nanodb.queryeval;

import java.io.IOException;
import java.util.*;

import edu.caltech.nanodb.expressions.*;
import edu.caltech.nanodb.plannodes.*;
import edu.caltech.nanodb.queryast.SelectClause;
import edu.caltech.nanodb.queryeval.CostBasedJoinPlanner;

/**
 * This class is used by the overall planner to handle sub-queries.
 */
public class SubqueryPlanner {

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
     */
    public void planSubquery(SubqueryOperator query,
                             List<SelectClause> enclosingSelects) throws IOException {
        SelectClause select = query.getSubquery();
        PlanNode plan = planner.makePlan(select, enclosingSelects);
        query.setSubqueryPlan(plan);
    }

}
