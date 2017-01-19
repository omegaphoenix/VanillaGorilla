package edu.caltech.nanodb.plannodes;


import java.util.*;
import java.io.*;

import edu.caltech.nanodb.expressions.*;
import edu.caltech.nanodb.queryeval.ColumnStats;
import edu.caltech.nanodb.queryeval.PlanCost;
import edu.caltech.nanodb.relations.Schema;
import edu.caltech.nanodb.relations.Tuple;
import edu.caltech.nanodb.relations.ColumnInfo;

import edu.caltech.nanodb.queryast.SelectValue;
import org.apache.log4j.Logger;


/**
 * PlanNode representing the <tt>SELECT</tt> clause in a SQL query.
 * This is the relational algebra Project operator.
 */
public class ProjectNode extends PlanNode {

    /** A logging object for reporting anything interesting that happens. **/
    private static Logger logger = Logger.getLogger(ProjectNode.class);


    /**
     * This is our guess of how many unique values will be produced by a
     * project expression, when we cannot determine it from the child plan's
     * statistics.
     */
    private static final int GUESS_NUM_UNIQUE_VALUES = 100;


    /** The schema of tuples produced by the subplan. */
    private Schema inputSchema;


    /** The cost of the input subplan. */
    private PlanCost inputCost;


    /** The new schema that this project node creates */
    private List<SelectValue> projectionSpec;


    /**
     * This flag is set to true if the project is a trivial projection, where
     * tuples are passed through unmodified.
     */
    private boolean projectIsTrivial;


    /**
     * This collection holds the non-wildcard column information, so that we can
     * more easily assign schema to projected tuples.
     */
    private List<ColumnInfo> nonWildcardColumnInfos;


    /** Current tuple the node is projecting (in NON-projected form). */
    private Tuple currentTuple;


    /** True if we have finished pulling tuples from children. */
    private boolean done;


    /**
     * Constructs a ProjectNode that pulls tuples from a child node.
     *
     * @param leftChild the child to pull tuples from
     * @param projectionSpec the set of expressions specifying how to project
     *                       input tuples.
     */
    public ProjectNode(PlanNode leftChild, List<SelectValue> projectionSpec) {
        // This node is a Project node.
        super(OperationType.PROJECT);

        // This node has a child, load child information.
        this.leftChild = leftChild;

        this.projectionSpec = projectionSpec;
    }


    /**
     * Constructs a "leaf ProjectNode"; i.e. one that doesn't pull tuples
     * from anywhere, but rather generates one tuple from its projection
     * specification and then stops.
     *
     * @param projectionSpec the set of expressions specifying how to project
     *                       input tuples.
     */
    public ProjectNode(List<SelectValue> projectionSpec) {
        this(null, projectionSpec);
        setTrivialFlag();
    }


    /**
     * This helper method sets the {@link #projectIsTrivial} flag based on
     * whether the passed-in projection specification is a single wildcard
     * operation that would allow tuples to pass through unmodified.
     */
    private void setTrivialFlag() {
        if (projectionSpec.size() == 1) {
            SelectValue sel = projectionSpec.get(0);
            if (sel.isWildcard()) {
                ColumnName wildcard = sel.getWildcard();
                if (wildcard.getTableName() == null) {
                    projectIsTrivial = true;
                }
            }
        }

        projectIsTrivial = false;
    }


    /**
     * Returns true if the project node is a trivial <tt>*</tt> projection
     * with no table references.  If the projection is trivial then it could
     * even be removed.
     *
     * @return true if the select value is a full wildcard value, not even
     *         specifying a table name
     */
    public boolean isTrivial() {
        return projectIsTrivial;
    }


    public void prepare() {
        if (leftChild != null) {
            // Need to prepare the left child-node before we can do our own
            // work.
            leftChild.prepare();

            // Use the helper function to prepare the schema of this
            // project-node, since it is a complicated operation.
            prepareSchemaStats(leftChild.getSchema(), leftChild.getStats());

            // Come up with a cost estimate now.  Projection does require some
            // computation, so increase the CPU cost based on the number of
            // tuples expected to come into this plan-node.

            inputCost = leftChild.getCost();
            if (inputCost != null) {
                cost = new PlanCost(inputCost);
                cost.cpuCost += inputCost.numTuples;
            }
            else {
                logger.debug(
                    "Child's cost not available; not computing this node's cost.");
            }
        }
        else {
            // The project operator is a leaf in the plan, so it must generate
            // only one tuple.

            // Prepare the schema and statistics using empty schema and stats
            // values.
            prepareSchemaStats(new Schema(), new ArrayList<ColumnStats>());

            // Estimated number of tuples:  1
            // Tuple size:  TODO
            // Estimated CPU cost:  1
            // Number of block IOs:  0
            inputCost = new PlanCost(1, /* TODO:  tupleSize */ 100, 1, 0);
            cost = inputCost;
        }

        // TODO:  Estimate the final tuple-size.  It isn't hard, just tedious.
    }


    /**
     * This helper function computes the schema of the project plan-node, based
     * on the schema of its child-plan, and also the expressions specified in
     * the project operation.
     */
    protected void prepareSchemaStats(Schema inputSchema,
                                      ArrayList<ColumnStats> inputStats) {
        this.inputSchema = inputSchema;

        schema = new Schema();
        nonWildcardColumnInfos = new ArrayList<ColumnInfo>();

        stats = new ArrayList<ColumnStats>();

        for (SelectValue selVal : projectionSpec) {
            if (selVal.isWildcard()) {
                ColumnName wildcard = selVal.getWildcard();
                if (wildcard.isTableSpecified()) {
                    // Need to find all columns that are associated with the
                    // specified table.
                    SortedMap<Integer, ColumnInfo> found =
                        inputSchema.findColumns(wildcard);

                    // Add each column that was found, as well as its stats.
                    schema.append(found.values());
                    for (Integer idx : found.keySet())
                        stats.add(inputStats.get(idx));
                }
                else {
                    // No table is specified, so this is all columns in the
                    // child schema.
                    schema.append(inputSchema);
                    stats.addAll(inputStats);
                }
            }
            else if (selVal.isExpression()) {
                // Determining the schema is relatively straightforward.  The
                // statistics, unfortunately, are a different matter:  if the
                // expression is a simple column-reference then we can look up
                // the stats from the subplan, but if the expression is an
                // arithmetic operation, we need to guess...

                Expression expr = selVal.getExpression();
                ColumnInfo colInfo;

                if (expr instanceof ColumnValue) {
                    // This is a simple column-reference.  Pull out the schema
                    // and the statistics from the input.
                    ColumnValue colValue = (ColumnValue) expr;
                    int colIndex = inputSchema.getColumnIndex(colValue.getColumnName());
                    colInfo = inputSchema.getColumnInfo(colIndex);
                    stats.add(inputStats.get(colIndex));
                }
                else {
                    // This is a more complicated expression.  Guess the schema,
                    // and assume that every row will have a distinct value.

                    colInfo = expr.getColumnInfo(inputSchema);

                    // TODO:  We could be more sophisticated about this...
                    ColumnStats colStat = new ColumnStats();

                    if (inputCost != null) {
                        // Adding 0.5 and casting to int is equivalent to
                        // rounding, as long as the input >= 0.
                        colStat.setNumUniqueValues(
                            (int) (inputCost.numTuples + 0.5f));
                    }
                    else {
                        colStat.setNumUniqueValues(GUESS_NUM_UNIQUE_VALUES);
                    }

                    stats.add(colStat);
                }

                // Apply any aliases here...
                String alias = selVal.getAlias();
                if (alias != null)
                    colInfo = new ColumnInfo(alias, colInfo.getType());

                schema.addColumnInfo(colInfo);
                nonWildcardColumnInfos.add(colInfo);
            }
            else if (selVal.isScalarSubquery()) {
                throw new UnsupportedOperationException(
                    "Scalar subquery support is currently incomplete.");
            }
        }
    }


    /** Determines whether the results of the node are sorted. */
    public List<OrderByExpression> resultsOrderedBy() {
        // TODO:  if subplan is ordered and projected results include the same
        //        columns, then this node's results are also ordered.
        return null;
    }


    /** This node supports marking if its subplan supports marking. */
    public boolean supportsMarking() {
        return leftChild != null && leftChild.supportsMarking();
    }


    /** The project node doesn't require any marking from either child. */
    public boolean requiresLeftMarking() {
        return false;
    }


    /** The project node doesn't require any marking from either child. */
    public boolean requiresRightMarking() {
        return false;
    }


    /**
     * Gets the next tuple and projects it.
     *
     * @return the tuple to be passed up to the next node.
     *
     * @throws java.io.IOException if a db file failed to open at some point
     */
    public Tuple getNextTuple() throws IOException {

        // If this node is finished finding tuples, return null until it is
        // re-initialized.
        if (done)
            return null;

        if (leftChild != null) {
            // Advance the left child to the next tuple.  If there are no more
            // tuples, the projection process is over - set the done flag.
            advanceCurrentTuple();
            if (currentTuple == null)
                done = true;
        }
        else {
            // Since we are here, we know that done == false and we haven't
            // generated our tuple yet.
            currentTuple = new TupleLiteral();
            done = true;
        }

        if (currentTuple != null)
            return projectTuple(currentTuple);

        done = true;
        return null;
    }


    /** Helper function that advances the current tuple reference in the node.
     *
     * @throws java.lang.IllegalStateException if this is a node no child.
     * @throws java.io.IOException if a db file failed to open at some point
     */
    private void advanceCurrentTuple() throws IOException {
        currentTuple = leftChild.getNextTuple();
    }


    /**
     * This helper method takes an input tuple and projects it to a result tuple
     * based on the project
     *
     * @param tuple the tuple to project
     *
     * @return the projected version of the tuple
     */
    private Tuple projectTuple(Tuple tuple) {

        // If the projection-spec is simply a single wildcard value, e.g.
        // "SELECT * FROM foo", then just return the tuple unmodified.
        if (isTrivial())
            return tuple;

        // The projection is *not* trivial, so we need to do some evaluatin'.

        environment.clear();
        environment.addTuple(inputSchema, tuple);

        // Create an empty tuple to add values to.
        TupleLiteral newTuple = new TupleLiteral();

        // For each select value, evaluate it and add it to the tuple.

        Iterator<ColumnInfo> iterNonWildcardCols =
            nonWildcardColumnInfos.iterator();

        for (SelectValue selVal : projectionSpec) {
            if (selVal.isWildcard()) {
                // This value is a wildcard.  Find the columns that match the
                // wildcard, then add their values one by one.

                // Wildcard expressions cannot rename their results.

                ColumnName wildcard = selVal.getWildcard();
                if (wildcard.isTableSpecified()) {
                    // Need to find all columns that are associated with the
                    // specified table.

                    SortedMap<Integer, ColumnInfo> matchCols =
                        inputSchema.findColumns(wildcard);

                    for (int iCol : matchCols.keySet())
                        newTuple.addValue(tuple.getColumnValue(iCol));
                }
                else {
                    // No table is specified, so this is all columns in the
                    // child schema.
                    newTuple.appendTuple(tuple);
                }
            }
            else if (selVal.isExpression()) {
                // This value is a simple expression.
                Expression expr = selVal.getExpression();
                String alias = selVal.getAlias();

                // Get the result of the projection for this value.

                Object result = expr.evaluate(environment);
                ColumnInfo colInfo = iterNonWildcardCols.next();

                logger.debug(String.format(
                    "Expression:  %s \tColInfo:  %s\tAlias:  %s",
                    expr, colInfo, alias));

                // Add the result to the tuple.

                /*
                if (alias != null)
                    colInfo = new ColumnInfo(alias, colInfo.getType());

                logger.debug(String.format(
                    "Result:  %s \tColInfo:  %s\n", result, colInfo));
                */

                newTuple.addValue(result);
            }
            else if (selVal.isScalarSubquery()) {
                throw new UnsupportedOperationException(
                    "Scalar subquery support is currently incomplete");
            }
            else {
                throw new IllegalStateException(
                    "Select-value doesn't specify a value");
            }
        }

        return newTuple;
    }


    /** Do initialization for the select operation.  Resets state variables. */
    public void initialize() {
        super.initialize();

        done = false;
        currentTuple = null;

        if (leftChild != null)
            leftChild.initialize();
    }


    public void markCurrentPosition() {
        if (leftChild != null)
            leftChild.markCurrentPosition();
        else
            throw new UnsupportedOperationException("Not supported");
    }


    public void resetToLastMark() {
        if (leftChild != null)
            leftChild.resetToLastMark();
        else
            throw new UnsupportedOperationException("Not supported");
    }


    public void cleanUp() {
        if (leftChild != null)
            leftChild.cleanUp();
    }


    /**
     * Checks if the argument is a plan node tree with the same structure,
     * but not necesarily the same references.
     *
     * @param obj the object to which we are comparing
     */
    @Override
    public boolean equals(Object obj) {

        if (obj instanceof ProjectNode) {
            ProjectNode other = (ProjectNode) obj;

            return projectionSpec.equals(other.projectionSpec) &&
                (leftChild == null && other.leftChild == null ||
                 leftChild != null && leftChild.equals(other.leftChild));
        }

        return false;
    }


    /** Computes and returns the hash-code of a project node. */
    @Override
    public int hashCode() {
        int hash = 7;
        hash = 31 * hash + projectionSpec.hashCode();
        hash = 31 * hash + (leftChild != null ? leftChild.hashCode() : 0);
        return hash;
    }


    /**
     * Returns a string representing this project node's details.
     *
     * @return a string representing this project-node.
     */
    @Override
    public String toString() {
        return "Project[values:  " + projectionSpec.toString() + "]";
    }


    /**
     * Creates a copy of this project node and its subtree.  This method is used
     * by {@link PlanNode#duplicate} to copy a plan tree.
     */
    @Override
    protected PlanNode clone() throws CloneNotSupportedException {
        ProjectNode node = (ProjectNode) super.clone();

        ArrayList<SelectValue> newList = new ArrayList<SelectValue>();
        for (SelectValue sel : this.projectionSpec) {
            SelectValue newSel = (SelectValue) sel.clone();
            newList.add(newSel);
        }
        node.projectionSpec = newList;

        return node;
    }
}
