package edu.caltech.nanodb.queryeval;


import java.io.IOException;
import java.util.List;
import java.util.ArrayList;

import edu.caltech.nanodb.plannodes.*;
import org.apache.log4j.Logger;

import edu.caltech.nanodb.queryast.FromClause;
import edu.caltech.nanodb.queryast.SelectClause;
import edu.caltech.nanodb.queryast.SelectValue;

import edu.caltech.nanodb.expressions.Expression;
import edu.caltech.nanodb.expressions.AggregateProcessor;

import edu.caltech.nanodb.plannodes.FileScanNode;
import edu.caltech.nanodb.plannodes.PlanNode;
import edu.caltech.nanodb.plannodes.ProjectNode;
import edu.caltech.nanodb.plannodes.SelectNode;
import edu.caltech.nanodb.plannodes.HashedGroupAggregateNode;

import edu.caltech.nanodb.plannodes.SimpleFilterNode;
import edu.caltech.nanodb.expressions.OrderByExpression;

import edu.caltech.nanodb.relations.TableInfo;


/**
 * This class generates execution plans for very simple SQL
 * <tt>SELECT * FROM tbl [WHERE P]</tt> queries.  The primary responsibility
 * is to generate plans for SQL <tt>SELECT</tt> statements, but
 * <tt>UPDATE</tt> and <tt>DELETE</tt> expressions will also use this class
 * to generate simple plans to identify the tuples to update or delete.
 */
public class SimplePlanner extends AbstractPlannerImpl {

    /** A logging object for reporting anything interesting that happens. */
    private static Logger logger = Logger.getLogger(SimplePlanner.class);


    /**
     * Returns the root of a plan tree suitable for executing the specified
     * query.
     *
     * @param selClause an object describing the query to be performed
     *
     * @return a plan tree for executing the specified query
     *
     * @throws IOException if an IO error occurs when the planner attempts to
     *         load schema and indexing information.
     */
    @Override
    public PlanNode makePlan(SelectClause selClause,
        List<SelectClause> enclosingSelects) throws IOException {
        AggregateProcessor aggregateProcessor = new AggregateProcessor(true);
        AggregateProcessor noAggregateProcessor = new AggregateProcessor(false);

        // PlanNode to return.
        PlanNode result = null;

        FromClause fromClause = selClause.getFromClause();
        Expression whereExpr = selClause.getWhereExpr();
        if (fromClause != null) {

            // If from clause is a base table, simply do a file scan.
            if (fromClause.isBaseTable()) {
                result = makeSimpleSelect(fromClause.getTableName(),
                        whereExpr, null);
            }
            // Handle joins.
            else if (fromClause.isJoinExpr()) {
                Expression e = fromClause.getOnExpression();
                if (e == null) {
                    e = fromClause.getComputedJoinExpr();
                }
                e.traverse(noAggregateProcessor);
                result = makeJoinPlan(selClause, fromClause);
            }
            // Handle derived table recursively.
            else if (fromClause.isDerivedTable()) {
                SelectClause subClause = fromClause.getSelectClause();

                // Enclosing selects for sub-query.
                List<SelectClause> enclosing = null;
                if (enclosingSelects != null) {
                    enclosing = new ArrayList<SelectClause>(enclosingSelects);
                    enclosing.add(selClause);
                }
                else {
                    enclosing = new ArrayList<SelectClause>();
                    enclosing.add(selClause);
                }
                result = makePlan(subClause, enclosing);
                result = new RenameNode(result, fromClause.getResultName());
            }
        }

        // Check to see for trivial project (SELECT * FROM ...)
        if (!selClause.isTrivialProject()) {
            List<SelectValue> selectValues = selClause.getSelectValues();

            for (SelectValue sv : selectValues) {
                if (!sv.isExpression())
                    continue;
                Expression e = sv.getExpression().traverse(aggregateProcessor);
                sv.setExpression(e);
            }

            Expression havingExpr = selClause.getHavingExpr();
            if (havingExpr != null) {
                Expression e = havingExpr.traverse(aggregateProcessor);
                selClause.setHavingExpr(e);
            }

            if (whereExpr != null) {
                whereExpr.traverse(noAggregateProcessor);
            }

            if (selClause.getGroupByExprs().size() != 0 || aggregateProcessor.aggregates.size() != 0) {
                result = new HashedGroupAggregateNode(result, selClause.getGroupByExprs(), aggregateProcessor.aggregates);
            }

            if (havingExpr != null) {
                result = new SimpleFilterNode(result, havingExpr);
            }

            // If there is no FROM clause, make a trivial ProjectNode()
            if (fromClause == null) {
                result = new ProjectNode(selectValues);
            }
            else {
                result = new ProjectNode(result, selectValues);
            }
            result.prepare();
        }

        List<OrderByExpression> orderByExprs = selClause.getOrderByExprs();
        if (orderByExprs.size() > 0) {
            result = new SortNode(result, orderByExprs);
            result.prepare();
        }

        return result;
    }

}

