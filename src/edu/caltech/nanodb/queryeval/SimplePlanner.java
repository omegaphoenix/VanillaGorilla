package edu.caltech.nanodb.queryeval;


import java.io.IOException;
import java.util.List;

import org.apache.log4j.Logger;

import edu.caltech.nanodb.queryast.FromClause;
import edu.caltech.nanodb.queryast.SelectClause;
import edu.caltech.nanodb.queryast.SelectValue;

import edu.caltech.nanodb.expressions.Expression;

import edu.caltech.nanodb.plannodes.FileScanNode;
import edu.caltech.nanodb.plannodes.PlanNode;
import edu.caltech.nanodb.plannodes.ProjectNode;
import edu.caltech.nanodb.plannodes.SelectNode;

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
        List<SelectClause> enclosingSelects) throws IOException {
        // PlanNode to return.
        PlanNode result = null;

        if (enclosingSelects != null && !enclosingSelects.isEmpty()) {
            throw new UnsupportedOperationException(
                "Not implemented:  enclosing queries");
        }

        // Check to see for trivial project (SELECT * FROM ...)
        List<SelectValue> selectValues = null;
        if (!selClause.isTrivialProject()) {
            selectValues = selClause.getSelectValues();
        }

        FromClause fromClause = selClause.getFromClause();
        // This if statement is to avoid duplicating null pointer checks.
        if (fromClause == null) {
        }
        // If from clause is a base table, simply do a file scan.
        else if (fromClause.isBaseTable()) {
            result = makeSimpleSelect(fromClause.getTableName(),
                                      selClause.getWhereExpr(), null);
        }
        else if (fromClause.isJoinExpr()) {
            if (fromClause.isOuterJoin()) {
                throw new UnsupportedOperationException(
                          "Not implemented: outer joins");
            }
            else {
                throw new UnsupportedOperationException(
                          "Not implemented: inner joins");
            }
        }
        else if (fromClause.isDerivedTable()) {
            throw new UnsupportedOperationException(
                      "Not implemented: subqueries in FROM clause");
        }

        // Project if necessary.
        if (selectValues != null) {
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
}

