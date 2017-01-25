package edu.caltech.nanodb.queryeval;

import java.io.IOException;
import java.util.List;

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
        List<SelectClause> enclosingSelects) throws IOException, IllegalArgumentException {
        AggregateProcessor processor = new AggregateProcessor();

        // PlanNode to return.
        PlanNode result = null;

        if (enclosingSelects != null && !enclosingSelects.isEmpty()) {
            throw new UnsupportedOperationException(
                "Not implemented:  enclosing queries");
        }
        FromClause fromClause = selClause.getFromClause();
        Expression whereExpr = selClause.getWhereExpr();
        if (fromClause != null) {

            if (fromClause.getClauseType() == FromClause.ClauseType.JOIN_EXPR) {
                Expression e = fromClause.getOnExpression();
                processor.resetCurrent();
                e.traverse(processor);
                if (processor.currentExprHasAggr) {
                    throw new IllegalArgumentException("Join on expression contains aggregate function");
                }
            }

            // If from clause is a base table, simply do a file scan.
            if (fromClause.isBaseTable()) {
                result = makeSimpleSelect(fromClause.getTableName(),
                        whereExpr, null);
            }
            else if (fromClause.isJoinExpr()) {
                result = makeJoinPlan(selClause, fromClause);
            }
            else if (fromClause.isDerivedTable()) {
                throw new UnsupportedOperationException(
                        "Not implemented: subqueries in FROM clause");
            }
        }

        // Check to see for trivial project (SELECT * FROM ...)
        if (!selClause.isTrivialProject()) {
            List<SelectValue> selectValues = selClause.getSelectValues();

            for (SelectValue sv : selectValues) {
                if (!sv.isExpression())
                    continue;
                processor.resetCurrent();
                Expression e = sv.getExpression().traverse(processor);
                sv.setExpression(e);
            }

            Expression havingExpr = selClause.getHavingExpr();
            processor.resetCurrent();
            if (havingExpr != null) {
                Expression e = havingExpr.traverse(processor);
                selClause.setHavingExpr(e);
            }

            if (whereExpr != null) {
                processor.resetCurrent();
                whereExpr.traverse(processor);
                if (processor.currentExprHasAggr) {
                    throw new IllegalArgumentException("Where expression contains aggregate function");
                }
            }
            result = new HashedGroupAggregateNode(result, selClause.getGroupByExprs(), processor.aggregates);

            // If there is no FROM clause, make a trivial ProjectNode()
            if (fromClause == null) {
                result = new ProjectNode(selectValues);
            }
            else {
                result = new ProjectNode(result, selectValues);
            }
            result.prepare();
        }
        return result;
    }

}

