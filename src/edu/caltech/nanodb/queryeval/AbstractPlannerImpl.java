package edu.caltech.nanodb.queryeval;


import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import edu.caltech.nanodb.expressions.*;
import edu.caltech.nanodb.plannodes.*;
import edu.caltech.nanodb.relations.ColumnInfo;
import org.apache.log4j.Logger;

import edu.caltech.nanodb.queryast.FromClause;
import edu.caltech.nanodb.queryast.SelectClause;
import edu.caltech.nanodb.queryast.SelectValue;

import edu.caltech.nanodb.relations.JoinType;
import edu.caltech.nanodb.relations.TableInfo;
import edu.caltech.nanodb.storage.StorageManager;


/**
 * This class contains implementation details that are common across all query
 * planners.  Planners are of course free to implement these operations
 * separately, but just about all planners have some common functionality, and
 * it's very helpful to implement that functionality once in an abstract base
 * class.
 */
public abstract class AbstractPlannerImpl implements Planner {
    /** A logging object for reporting anything interesting that happens. */
    private static Logger logger = Logger.getLogger(AbstractPlannerImpl.class);


    /** The storage manager used during query planning. */
    protected StorageManager storageManager;


    /** The next placeholder table name number */
    private int placeholder_num = 0;


    /** Sets the storage manager to be used during query planning. */
    public void setStorageManager(StorageManager storageManager) {
        this.storageManager = storageManager;
    }

    /**
     * Constructs a simple select plan that reads directly from a table, with
     * an optional predicate for selecting rows.
     * <p>
     * While this method can be used for building up larger <tt>SELECT</tt>
     * queries, the returned plan is also suitable for use in <tt>UPDATE</tt>
     * and <tt>DELETE</tt> command evaluation.  In these cases, the plan must
     * only generate tuples of type {@link edu.caltech.nanodb.storage.PageTuple},
     * so that the command can modify or delete the actual tuple in the file's
     * page data.
     *
     * @param tableName The name of the table that is being selected from.
     *
     * @param predicate An optional selection predicate, or {@code null} if
     *        no filtering is desired.
     *
     * @return A new plan-node for evaluating the select operation.
     *
     * @throws IOException if an error occurs when loading necessary table
     *         information.
     */
    public SelectNode makeSimpleSelect(String tableName, Expression predicate,
        List<SelectClause> enclosingSelects) throws IOException {
        if (tableName == null)
            throw new IllegalArgumentException("tableName cannot be null");

        if (enclosingSelects != null) {
            // If there are enclosing selects, this subquery's predicate may
            // reference an outer query's value, but we don't detect that here.
            // Therefore we will probably fail with an unrecognized column
            // reference.
            logger.warn("Currently we are not clever enough to detect " +
                "correlated subqueries, so expect things are about to break...");
        }

        // Open the table.
        TableInfo tableInfo = storageManager.getTableManager().openTable(tableName);

        // Make a SelectNode to read rows from the table, with the specified
        // predicate.
        SelectNode selectNode = new FileScanNode(tableInfo, predicate);
        selectNode.prepare();
        return selectNode;
    }

    /**
     * Make a plan for the join expression.
     *
     *
     * @param selClause an object describing the query to be performed.
     * @param fromClause an object describing the FROM clause in a query.
     *
     * @return a plan tree for executing the specified joins.
     *
     * @throws IOException if an IO error occurs when the planner attempts to
     *         load schema and indexing information.
     */
    public PlanNode makeJoinPlan(SelectClause selClause,
                                 FromClause fromClause) throws IOException {
        PlanNode resPlan = null;
        // Base case for recursion.
        if (fromClause.isBaseTable()) {
            resPlan = makeSimpleSelect(fromClause.getTableName(), null, null);

            if (fromClause.isRenamed()) {
                resPlan = new RenameNode(resPlan, fromClause.getResultName());
            }
            resPlan.prepare();
            return resPlan;
        }
        else if (fromClause.isDerivedTable()) {
            List<SelectClause> enclosing = new ArrayList<SelectClause>();
            enclosing.add(selClause);
            resPlan = makePlan(fromClause.getSelectClause(), enclosing);
            resPlan = new RenameNode(resPlan, fromClause.getResultName());
            return resPlan;
        }

        JoinType joinType = fromClause.getJoinType();
        PlanNode left = makeJoinPlan(selClause, fromClause.getLeftChild());
        PlanNode right = makeJoinPlan(selClause, fromClause.getRightChild());
        FromClause.JoinConditionType condType = fromClause.getConditionType();
        Expression predicate;
        List<SelectValue> projectVals = null;
        boolean needPostProject =
                condType == FromClause.JoinConditionType.NATURAL_JOIN ||
                condType == FromClause.JoinConditionType.JOIN_USING;
        if (needPostProject) {
            predicate = fromClause.getComputedJoinExpr();
        }
        else {
            predicate = fromClause.getOnExpression();
        }
        switch (joinType) {
            case CROSS:
            case INNER:
            case LEFT_OUTER:
            case ANTIJOIN:
            case SEMIJOIN:
                resPlan = new NestedLoopJoinNode(left, right, joinType, predicate);
                break;
            case RIGHT_OUTER:
                resPlan = new NestedLoopJoinNode(left, right, JoinType.LEFT_OUTER, predicate);
                ((ThetaJoinNode) resPlan).swap();
                break;
            case FULL_OUTER:
                throw new UnsupportedOperationException(
                          "Not implemented: FULL_OUTER join");
            default:
                throw new UnsupportedOperationException("Not a valid JoinType.");
        }
        if (needPostProject) {
            projectVals = fromClause.getComputedSelectValues();
            if (projectVals != null) {
                resPlan = new ProjectNode(resPlan, projectVals, placeholder_num);
                placeholder_num++;
            }
        }
        resPlan.prepare();
        return resPlan;
    }

    public SelectClause decorrelate(SelectClause selClause) {
        if (isDecorrelatableIn(selClause)) {
            // Decorrelate
            selClause = decorrelateIn(selClause);
        }
        else if (isDecorrelatableExists(selClause)) {
            // Decorrelate
            selClause = decorrelateExists(selClause);
        }
        return selClause;
    }

    public SelectClause decorrelateIn(SelectClause selClause) {
        FromClause leftFrom = selClause.getFromClause();
        Expression whereExpr = selClause.getWhereExpr();
        SelectClause subquery = ((InSubqueryOperator) whereExpr).getSubquery();
        FromClause rightFrom = subquery.getFromClause();
        FromClause newFromClause =
                new FromClause(leftFrom, rightFrom, JoinType.SEMIJOIN);
        Expression newOnExpression = subquery.getWhereExpr();
        newFromClause.setOnExpression(newOnExpression);
        selClause.setFromClause(newFromClause);
        selClause.setWhereExpr(null);
        return selClause;
    }

    public SelectClause decorrelateExists(SelectClause selClause) {
        Expression whereExpr = selClause.getWhereExpr();
        ExistsOperator existsOperator = (ExistsOperator) whereExpr;
        SelectClause sel = existsOperator.getSubquery();

        if (sel.isCorrelated()) {
            Expression condition = sel.getWhereExpr();
            sel.setWhereExpr(null);
            sel.clearCorrelated();

            FromClause left = selClause.getFromClause();
            FromClause right = new FromClause(sel,"alias");
            FromClause newFrom = new FromClause(left, right, JoinType.SEMIJOIN);
            newFrom.setOnExpression(condition);

            selClause.setFromClause(newFrom);
        }

        return selClause;
    }

    public boolean isDecorrelatableIn(SelectClause selClause) {
        // Check if subquery inside where clause
        Expression whereExpr = selClause.getWhereExpr();
        if (whereExpr != null) {
            if (whereExpr instanceof InSubqueryOperator) {
                return ((InSubqueryOperator) whereExpr).getSubquery().isCorrelated();
            }
        }
        return false;
    }

    public boolean isDecorrelatableExists(SelectClause selClause) {
        // Check if subquery inside where clause
        Expression whereExp = selClause.getWhereExpr();

        if (whereExp instanceof ExistsOperator) {
            return true;
        }
        return false;
    }
}
