package edu.caltech.nanodb.plannodes;


import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import edu.caltech.nanodb.expressions.Environment;
import edu.caltech.nanodb.expressions.ExpressionException;
import edu.caltech.nanodb.expressions.TupleLiteral;
import org.apache.log4j.Logger;

import edu.caltech.nanodb.expressions.ColumnName;
import edu.caltech.nanodb.expressions.ColumnValue;
import edu.caltech.nanodb.expressions.Expression;
import edu.caltech.nanodb.expressions.FunctionCall;

import edu.caltech.nanodb.functions.AggregateFunction;
import edu.caltech.nanodb.functions.MinMaxAggregate;

import edu.caltech.nanodb.queryeval.ColumnStats;

import edu.caltech.nanodb.relations.ColumnInfo;
import edu.caltech.nanodb.relations.Schema;


/**
 * This class provides the common functionality necessary for grouping and
 * aggregation.  Concrete subclasses implement grouping and aggregation using
 * different strategies.
 */
public abstract class GroupAggregateNode extends PlanNode {

    /** A logging object for reporting anything interesting that happens. */
    private static Logger logger = Logger.getLogger(GroupAggregateNode.class);


    /**
     * The schema of the plan-node feeding this grouping/aggregate node.  We
     * use it a lot, so it makes sense to have it cached.
     */
    protected Schema inputSchema;


    /**
     * A list of zero or more expressions that the grouping/aggregate node
     * will use for generating aggregates over.  If the list is empty then
     * the aggregates will be computed over the entire input data-set.
     */
    protected List<Expression> groupByExprs;


    /**
     * A list of one or more aggregate functions to compute over the input
     * data.
     */
    protected Map<String, FunctionCall> aggregates;


    /**
     * The estimated number of tuples this plan-node produces.  Cached to
     * make plan-costing easier.
     */
    protected int estimatedNumTuples;


    protected GroupAggregateNode(PlanNode subplan,
        List<Expression> groupByExprs, Map<String, FunctionCall> aggregates) {

        super(PlanNode.OperationType.GROUP_AGGREGATE, subplan);

        if (groupByExprs == null)
            throw new IllegalArgumentException("groupByExprs cannot be null");

        if (aggregates == null)
            throw new IllegalArgumentException("aggregates cannot be null");

        /*
        if (aggregates.size() == 0) {
            throw new IllegalArgumentException("aggregates must specify at " +
                "least one aggregate function to compute");
        }
        */

        this.groupByExprs = groupByExprs;
        this.aggregates = aggregates;
    }


    /**
     * This helper function computes the schema of the grouping/aggregate
     * plan-node, based on the schema of its child-plan, and also the
     * expressions specified in the grouping/aggregate operation.
     */
    protected void prepareSchemaStats() {
        inputSchema = leftChild.getSchema();
        ArrayList<ColumnStats> inputStats = leftChild.getStats();

        schema = new Schema();
        stats = new ArrayList<ColumnStats>();
        int numTuples = 1;

        // Add columns based on the GROUP BY expression and SELECT values.
        // After grouping, the only columns that appear in the result are those
        // specified by GROUP BY and new ones containing aggregates.
        for (Expression expr : groupByExprs) {
            ColumnInfo colInfo = expr.getColumnInfo(inputSchema);
            schema.addColumnInfo(colInfo);

            // Compute statistics for the grouping column.
            if (expr instanceof ColumnValue) {
                // This is a simple column-reference.  Pull out the statistics
                // from the input.  For grouping columns, the stats will be
                // the same, except the number of NULL values is at most 1.
                ColumnValue colValue = (ColumnValue) expr;
                int colIndex = inputSchema.getColumnIndex(colValue.getColumnName());
                // colInfo = inputSchema.getColumnInfo(colIndex);
                ColumnStats colStat = inputStats.get(colIndex);
                numTuples *= colStat.getNumUniqueValues();
                colStat.setNumNullValues(
                    (int) Math.signum(colStat.getNumNullValues()));
                stats.add(colStat);
            }
            else {
                throw new UnsupportedOperationException("NanoDB does not " +
                    "yet support GROUP BY expressions that are not simple " +
                    "column references; got " + expr);
            }
        }

        // Add columns for aggregate function calls.
        for (String name : aggregates.keySet()) {
            FunctionCall call = aggregates.get(name);
            AggregateFunction aggFn = (AggregateFunction) call.getFunction();
            List<Expression> args = call.getArguments();

            // Make a column-info object for the aggregate function's return
            // value.  We do this in two steps so we can set the name to the
            // generated internal name.
            ColumnInfo colInfo = call.getColumnInfo(inputSchema);
            colInfo = new ColumnInfo(name, colInfo.getType());

            ColumnStats colStat = new ColumnStats();

            // Compute statistics for the aggregate column.

            if (aggFn instanceof MinMaxAggregate && args.size() == 1 &&
                args.get(0) instanceof ColumnValue) {

                // We can be a bit more clever with MIN(a) and MAX(a):  the
                // min and max must come from the set of existing values, so
                // if the number of distinct values is less than the total
                // number of tuples, we can use the smaller value as our
                // estimate.

                ColumnValue colValue = (ColumnValue) args.get(0);
                ColumnName colName = colValue.getColumnName();
                int colIndex = inputSchema.getColumnIndex(colName);

                int numUniqueValues = inputStats.get(colIndex).getNumUniqueValues();

                colStat.setNumUniqueValues(Math.min(numUniqueValues, numTuples));
            }
            else {
                // For anything else, just guess that the aggregate function
                // will produces a different value for each group.
                colStat.setNumUniqueValues(numTuples);
            }

            stats.add(colStat);
            schema.addColumnInfo(colInfo);
        }

        estimatedNumTuples = numTuples;

        logger.info("Grouping/aggregate node schema:  " + schema);
        logger.info("Grouping/aggregate node stats:  " + stats);
        logger.info("Grouping/aggregate node estimated tuples:  " +
                    estimatedNumTuples);
    }


    /**
     * <p>
     * This helper method computes the value of each group-by expression and
     * returns a {@link TupleLiteral} containing the results.  If there are no
     * group-by expressions (because the node is computing its aggregates over
     * the entire input), the method returns {@code null}.
     * </p>
     * <p>
     * This method uses the plan node's {@link Environment} object to evaluate
     * the group-by expressions against; it expects that the environment has
     * already been properly initialized before it is called.
     * </p>
     *
     * @return a {@code TupleLiteral} object containing the results of
     *         evaluating the group-by expressions, or {@code null} if there
     *         are no group-by expressions for this node.
     */
    protected TupleLiteral evaluateGroupByExprs() {
        if (groupByExprs.isEmpty())
            return null;

        TupleLiteral result = new TupleLiteral();

        // Compute each group-by value and add it to the result tuple.
        for (Expression expr : groupByExprs)
            result.addValue(expr.evaluate(environment));

        return result;
    }


    /**
     * This helper method iterates through a collection of aggregate
     * functions, and clears (reinitializes) each aggregate's internal state.
     *
     * @param groupAggregates the collection of aggregates to clear
     */
    protected void clearAggregates(Map<String, FunctionCall> groupAggregates) {
        for (FunctionCall fnCall : groupAggregates.values()) {
            AggregateFunction aggFn = (AggregateFunction) fnCall.getFunction();
            aggFn.clearResult();
        }
    }


    /**
     * This helper method updates a collection of aggregate functions with
     * values based on the state of the plan node's current
     * {@link Environment} object.  Of course, the method expects that the
     * environment has already been properly initialized before it is called.
     *
     * @param groupAggregates the collection of aggregates to update
     *
     * @throws ExpressionException if an error is encountered while evaluating
     *         expressions for the aggregate operations.
     */
    protected void updateAggregates(Map<String, FunctionCall> groupAggregates)
        throws ExpressionException {
        for (String name : groupAggregates.keySet()) {
            FunctionCall call = groupAggregates.get(name);
            AggregateFunction aggFn = (AggregateFunction) call.getFunction();
            List<Expression> args = call.getArguments();

            if (args.size() != 1) {
                throw new ExpressionException("Aggregate functions " +
                    "currently require exactly one argument.");
            }
            Expression arg = args.get(0);

            Object value = arg.evaluate(environment);
            aggFn.addValue(value);

            // logger.debug("Argument to aggregate function = " + value +
            //     ", new aggregate result = " + aggFn.getResult());
        }
    }


    protected TupleLiteral generateOutputTuple(TupleLiteral groupValues,
        Map<String, FunctionCall> groupAggregates) {

        // logger.info("Group values:  " + groupValues);

        // Construct the result tuple from the group, and from the
        // computed aggregate values.
        TupleLiteral result = new TupleLiteral();
        if (groupValues != null) {
            result.appendTuple(groupValues);
        }

        // TODO:  Add the aggregate values in an order that matches what
        //        the grouping/aggregate plan node must output.
        for (String name : groupAggregates.keySet()) {
            FunctionCall fnCall = groupAggregates.get(name);
            AggregateFunction aggFn = (AggregateFunction) fnCall.getFunction();

            result.addValue(aggFn.getResult());
        }

        return result;
    }
}
