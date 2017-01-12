package edu.caltech.nanodb.queryeval;


import java.io.IOException;
import java.util.List;

import edu.caltech.nanodb.queryast.SelectClause;
import edu.caltech.nanodb.expressions.Expression;
import edu.caltech.nanodb.plannodes.PlanNode;
import edu.caltech.nanodb.plannodes.SelectNode;
import edu.caltech.nanodb.storage.StorageManager;


/**
 * <p>
 * This interface specifies the common entry-point for all query
 * planner/optimizer implementations.  The interface is very simple, but a
 * particular implementation might be very complicated depending on what kinds
 * of optimizations are implemented.  Note that a new planner/optimizer is
 * created for each query being planned
 * </p>
 * <p>
 * To support initialization of arbitrary planners, a {@code Planner}
 * implementation should only have a default constructor.  The
 * {@link PlannerFactory} class is used to instantiate planners.
 * </p>
 */
public interface Planner {
    /**
     * Allows the storage manager to be injected into the planner
     * implementation, so that the planner can retrieve schema and statistics
     * on tables that are referenced by the query.
     *
     * @param storageManager the storage manager instance for the planner
     *        implementation to use
     */
    void setStorageManager(StorageManager storageManager);

    /**
     * Returns the root of a plan tree suitable for executing the specified
     * query.  There is no requirement that tuples produced by the returned plan
     * should support updating or deletion.
     *
     * @param selClause an object describing the query to be performed
     *
     * @param enclosingSelects a list of enclosing queries, if {@code selClause}
     *        is a nested subquery.  This is allowed to be an empty list, or
     *        {@code null}, if the query is a top-level query.  If
     *        {@code selClause} is a nested subquery, {@code enclosingSelects[0]}
     *        is the outermost enclosing query, then {@code enclosingSelects[1]}
     *        is enclosed by {@code enclosingSelects[0]}, and so forth.  The
     *        most tightly enclosing query is the last one in
     *        {@code enclosingSelects}.
     *
     * @return a plan tree for executing the specified query
     *
     * @throws IOException if an error occurs when the planner is generating a
     *         plan (e.g. because statistics, schema, or indexing information
     *         can't be loaded)
     */
    PlanNode makePlan(SelectClause selClause, List<SelectClause> enclosingSelects)
        throws IOException;


    /**
     * Returns a plan tree for executing a simple select against a single table,
     * whose tuples can also be used for updating and deletion.  This is a
     * strict requirement, as this method is used by the
     * {@link edu.caltech.nanodb.commands.UpdateCommand} and
     * {@link edu.caltech.nanodb.commands.DeleteCommand} classes to create the
     * plans suitable for performing tuple modification or deletion.
     *
     * @param tableName the table that the select will operate against
     *
     * @param predicate the selection predicate to apply, or {@code null} if
     *        all tuples in the table should be returned
     *
     * @param enclosingSelects a list of enclosing queries, if {@code selClause}
     *        is a nested subquery.  This is allowed to be an empty list, or
     *        {@code null}, if the query is a top-level query.  If
     *        {@code selClause} is a nested subquery, {@code enclosingSelects[0]}
     *        is the outermost enclosing query, then {@code enclosingSelects[1]}
     *        is enclosed by {@code enclosingSelects[0]}, and so forth.  The
     *        most tightly enclosing query is the last one in
     *        {@code enclosingSelects}.
     *
     * @return a plan tree for executing the select operation
     *
     * @throws IOException if an error occurs when the planner is generating a
     *         plan (e.g. because statistics, schema, or indexing information
     *         can't be loaded)
     */
    SelectNode makeSimpleSelect(String tableName, Expression predicate,
                                List<SelectClause> enclosingSelects)
        throws IOException;
}
