package edu.caltech.nanodb.queryast;


import java.io.IOException;

import org.apache.log4j.Logger;

import edu.caltech.nanodb.expressions.Expression;
import edu.caltech.nanodb.expressions.ExpressionProcessor;
import edu.caltech.nanodb.expressions.SubqueryOperator;
import edu.caltech.nanodb.storage.TableManager;


/**
 * <p>
 * This expression-processor implementation looks for subqueries within an
 * expression, and calls the {@link SelectClause#computeSchema} method on them.
 * The enclosing query is passed as an argument, in case subqueries are
 * correlated with the enclosing query.
 * </p>
 * <p>
 * Note that this traversal mechanism will only identify the subqueries that
 * are immediately nested within this query; if a subquery itself has
 * subqueries, those will not be identified by this class, but rather will be
 * processed when the subquery's schema is computed.
 * </p>
 */
class SubquerySchemaComputer implements ExpressionProcessor {
    /** A logging object for reporting anything interesting that happens. **/
    private static Logger logger = Logger.getLogger(SubquerySchemaComputer.class);


    /** This is the parent query that may contain subqueries. */
    private SelectClause selectClause;

    /** This is the table-manager used to access table schemas. */
    private TableManager tableManager;


    /**
     * A flag used to track whether subqueries were found while
     * traversing the expression tree.
     */
    private boolean found = false;


    public SubquerySchemaComputer(SelectClause selectClause,
                                  TableManager tableManager) {
        this.selectClause = selectClause;
        if (tableManager == null)
            throw new IllegalArgumentException("tableManager cannot be null");

        this.tableManager = tableManager;
        // this.enclosingSelects = enclosingSelects;
    }


    /**
     * This method identifies {@link SubqueryOperator} objects, retrieves the
     * subquery, and then generates a plan for the subquery.
     *
     * @param e the expression node being entered
     */
    public void enter(Expression e) {
        if (e instanceof SubqueryOperator) {
            SubqueryOperator subqueryOp = (SubqueryOperator) e;
            SelectClause subquery = subqueryOp.getSubquery();

            try {
                // This operation requires the enclosing queries' schemas,
                // so that we can detect and handle correlated subqueries
                // properly.
                logger.debug(String.format(
                    "Computing schema for nested subquery %s", subquery));
                subquery.computeSchema(tableManager, selectClause);
            }
            catch (IOException ex) {
                throw new RuntimeException(
                    "Couldn't compute schema for subquery", ex);
            }

            found = true;
        }
    }

    /**
     * This method is a no-op for this expression processor.
     *
     * @param e the expression node being left
     *
     * @return the passed-in expression {@code e}.
     */
    public Expression leave(Expression e) {
        return e;  // No-op!
    }

    public boolean foundSubqueries() {
        return found;
    }

    public void clearFoundFlag() {
        found = false;
    }
}
