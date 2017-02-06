package edu.caltech.nanodb.queryeval;


import java.io.IOException;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import edu.caltech.nanodb.expressions.AggregateProcessor;
import edu.caltech.nanodb.expressions.OrderByExpression;
import edu.caltech.nanodb.expressions.PredicateUtils;
import edu.caltech.nanodb.plannodes.*;
import edu.caltech.nanodb.queryast.SelectValue;
import edu.caltech.nanodb.relations.JoinType;
import org.apache.log4j.Logger;

import edu.caltech.nanodb.expressions.Expression;
import edu.caltech.nanodb.queryast.FromClause;
import edu.caltech.nanodb.queryast.SelectClause;
import edu.caltech.nanodb.relations.TableInfo;


/**
 * This planner implementation uses dynamic programming to devise an optimal
 * join strategy for the query.  As always, queries are optimized in units of
 * <tt>SELECT</tt>-<tt>FROM</tt>-<tt>WHERE</tt> subqueries; optimizations
 * don't currently span multiple subqueries.
 */
public class CostBasedJoinPlanner extends AbstractPlannerImpl {

    /** A logging object for reporting anything interesting that happens. */
    private static Logger logger = Logger.getLogger(CostBasedJoinPlanner.class);


    /**
     * This helper class is used to keep track of one "join component" in the
     * dynamic programming algorithm.  A join component is simply a query plan
     * for joining one or more leaves of the query.
     * <p>
     * In this context, a "leaf" may either be a base table or a subquery in
     * the <tt>FROM</tt>-clause of the query.  However, the planner will
     * attempt to push conjuncts down the plan as far as possible, so even if
     * a leaf is a base table, the plan may be a bit more complex than just a
     * single file-scan.
     */
    private static class JoinComponent {
        /**
         * This is the join plan itself, that joins together all leaves
         * specified in the {@link #leavesUsed} field.
         */
        public PlanNode joinPlan;

        /**
         * This field specifies the collection of leaf-plans that are joined by
         * the plan in this join-component.
         */
        public HashSet<PlanNode> leavesUsed;

        /**
         * This field specifies the collection of all conjuncts use by this join
         * plan.  It allows us to easily determine what join conjuncts still
         * remain to be incorporated into the query.
         */
        public HashSet<Expression> conjunctsUsed;

        /**
         * Constructs a new instance for a <em>leaf node</em>.  It should not
         * be used for join-plans that join together two or more leaves.  This
         * constructor simply adds the leaf-plan into the {@link #leavesUsed}
         * collection.
         *
         * @param leafPlan the query plan for this leaf of the query.
         *
         * @param conjunctsUsed the set of conjuncts used by the leaf plan.
         *        This may be an empty set if no conjuncts apply solely to
         *        this leaf, or it may be nonempty if some conjuncts apply
         *        solely to this leaf.
         */
        public JoinComponent(PlanNode leafPlan, HashSet<Expression> conjunctsUsed) {
            leavesUsed = new HashSet<>();
            leavesUsed.add(leafPlan);

            joinPlan = leafPlan;

            this.conjunctsUsed = conjunctsUsed;
        }

        /**
         * Constructs a new instance for a <em>non-leaf node</em>.  It should
         * not be used for leaf plans!
         *
         * @param joinPlan the query plan that joins together all leaves
         *        specified in the <tt>leavesUsed</tt> argument.
         *
         * @param leavesUsed the set of two or more leaf plans that are joined
         *        together by the join plan.
         *
         * @param conjunctsUsed the set of conjuncts used by the join plan.
         *        Obviously, it is expected that all conjuncts specified here
         *        can actually be evaluated against the join plan.
         */
        public JoinComponent(PlanNode joinPlan, HashSet<PlanNode> leavesUsed,
                             HashSet<Expression> conjunctsUsed) {
            this.joinPlan = joinPlan;
            this.leavesUsed = leavesUsed;
            this.conjunctsUsed = conjunctsUsed;
        }

        public PlanNode getLeafPlan() {
            if (leavesUsed.size() != 1) {
                throw new IllegalStateException("JoinComponent is not single leaf");
            }
            else {
                return leavesUsed.iterator().next();
            }
        }
    }


    /**
     * Returns the root of a plan tree suitable for executing the specified
     * query.
     *
     * @param selClause an object describing the query to be performed
     *
     * @return a plan tree for executing the specified query
     *
     * @throws java.io.IOException if an IO error occurs when the planner attempts to
     *         load schema and indexing information.
     */
    public PlanNode makePlan(SelectClause selClause,
        List<SelectClause> enclosingSelects) throws IOException {

        // TODO:  Implement!
        AggregateProcessor aggregateProcessor = new AggregateProcessor(true);
        AggregateProcessor noAggregateProcessor = new AggregateProcessor(false);

        // PlanNode to return.
        PlanNode result = null;
        //
        // This is a very rough sketch of how this function will work,
        // focusing mainly on join planning:
        //
        // 1)  Pull out the top-level conjuncts from the WHERE and HAVING
        //     clauses on the query, since we will handle them in special ways
        //     if we have outer joins.
        FromClause fromClause = selClause.getFromClause();
        Expression whereExpr = selClause.getWhereExpr();
        //
        // 2)  Create an optimal join plan from the top-level from-clause and
        //     the top-level conjuncts.
        //
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
                if (e != null) {
                    e.traverse(noAggregateProcessor);
                }
                result = makeJoinPlan(selClause, fromClause);
                if (whereExpr != null) {
                    result = new SimpleFilterNode(result, whereExpr);
                }
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
        // 3)  If there are any unused conjuncts, determine how to handle them.
        //
        // 4)  Create a project plan-node if necessary.
        //
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
        }
        // 5)  Handle other clauses such as ORDER BY, LIMIT/OFFSET, etc.
        List<OrderByExpression> orderByExprs = selClause.getOrderByExprs();
        if (orderByExprs.size() > 0) {
            result = new SortNode(result, orderByExprs);
        }

        int limit = selClause.getLimit();
        int offset = selClause.getOffset();
        if (limit != 0 || offset != 0) {
            result = new LimitOffsetNode(result, selClause.getOffset(), selClause.getLimit());
        }

        result.prepare();
        return result;
    }


    /**
     * Given the top-level {@code FromClause} for a SELECT-FROM-WHERE block,
     * this helper generates an optimal join plan for the {@code FromClause}.
     *
     * @param fromClause the top-level {@code FromClause} of a
     *        SELECT-FROM-WHERE block.
     * @param extraConjuncts any extra conjuncts (e.g. from the WHERE clause,
     *        or HAVING clause)
     * @return a {@code JoinComponent} object that represents the optimal plan
     *         corresponding to the FROM-clause
     * @throws IOException if an IO error occurs during planning.
     */
    private JoinComponent makeJoinPlan(FromClause fromClause,
        Collection<Expression> extraConjuncts) throws IOException {

        // These variables receive the leaf-clauses and join conjuncts found
        // from scanning the sub-clauses.  Initially, we put the extra conjuncts
        // into the collection of conjuncts.
        HashSet<Expression> conjuncts = new HashSet<>();
        ArrayList<FromClause> leafFromClauses = new ArrayList<>();

        collectDetails(fromClause, conjuncts, leafFromClauses);

        logger.debug("Making join-plan for " + fromClause);
        logger.debug("    Collected conjuncts:  " + conjuncts);
        logger.debug("    Collected FROM-clauses:  " + leafFromClauses);
        logger.debug("    Extra conjuncts:  " + extraConjuncts);

        if (extraConjuncts != null)
            conjuncts.addAll(extraConjuncts);

        // Make a read-only set of the input conjuncts, to avoid bugs due to
        // unintended side-effects.
        Set<Expression> roConjuncts = Collections.unmodifiableSet(conjuncts);

        // Create a subplan for every single leaf FROM-clause, and prepare the
        // leaf-plan.

        logger.debug("Generating plans for all leaves");
        ArrayList<JoinComponent> leafComponents = generateLeafJoinComponents(
            leafFromClauses, roConjuncts);

        // Print out the results, for debugging purposes.
        if (logger.isDebugEnabled()) {
            for (JoinComponent leaf : leafComponents) {
                logger.debug("    Leaf plan:\n" +
                    PlanNode.printNodeTreeToString(leaf.joinPlan, true));
            }
        }

        // Build up the full query-plan using a dynamic programming approach.

        JoinComponent optimalJoin =
            generateOptimalJoin(leafComponents, roConjuncts);

        PlanNode plan = optimalJoin.joinPlan;
        logger.info("Optimal join plan generated:\n" +
            PlanNode.printNodeTreeToString(plan, true));

        return optimalJoin;
    }


    /**
     * This helper method pulls the essential details for join optimization
     * out of a <tt>FROM</tt> clause.
     * Add to leafFromClauses if fromClause is a leaf. Otherwise, collect
     * conjuncts from predicates (the on-expression and join-expression)
     * using collectConjuncts() method which breaks apart boolean AND
     * expressions and stores each individual term as a conjunct and
     * other predicates as a single conjunct.
     *
     * @param fromClause the from-clause to collect details from
     *
     * @param conjuncts the collection to add all conjuncts to
     *
     * @param leafFromClauses the collection to add all leaf from-clauses to
     */
    private void collectDetails(FromClause fromClause,
        HashSet<Expression> conjuncts, ArrayList<FromClause> leafFromClauses) {

        if (fromClause.isLeaf()) {
            leafFromClauses.add(fromClause);
        }
        else {
            // Collect conjuncts
            PredicateUtils.collectConjuncts(fromClause.getOnExpression(), conjuncts);
            PredicateUtils.collectConjuncts(fromClause.getComputedJoinExpr(), conjuncts);

            // Recursive call on child nodes
            if (fromClause.getLeftChild() != null) {
                collectDetails(fromClause.getLeftChild(), conjuncts, leafFromClauses);
            }
            if (fromClause.getRightChild() != null) {
                collectDetails(fromClause.getRightChild(), conjuncts, leafFromClauses);
            }
        }
    }



    /**
     * This helper method performs the first step of the dynamic programming
     * process to generate an optimal join plan, by generating a plan for every
     * leaf from-clause identified from analyzing the query.  Leaf plans are
     * usually very simple; they are built either from base-tables or
     * <tt>SELECT</tt> subqueries.  The most complex detail is that any
     * conjuncts in the query that can be evaluated solely against a particular
     * leaf plan-node will be associated with the plan node.  <em>This is a
     * heuristic</em> that usually produces good plans (and certainly will for
     * the current state of the database), but could easily interfere with
     * indexes or other plan optimizations.
     *
     * @param leafFromClauses the collection of from-clauses found in the query
     *
     * @param conjuncts the collection of conjuncts that can be applied at this
     *                  level
     *
     * @return a collection of {@link JoinComponent} object containing the plans
     *         and other details for each leaf from-clause
     *
     * @throws IOException if a particular database table couldn't be opened or
     *         schema loaded, for some reason
     */
    private ArrayList<JoinComponent> generateLeafJoinComponents(
        Collection<FromClause> leafFromClauses, Collection<Expression> conjuncts)
        throws IOException {

        // Create a subplan for every single leaf FROM-clause, and prepare the
        // leaf-plan.
        ArrayList<JoinComponent> leafComponents = new ArrayList<>();
        for (FromClause leafClause : leafFromClauses) {
            HashSet<Expression> leafConjuncts = new HashSet<>();

            PlanNode leafPlan =
                makeLeafPlan(leafClause, conjuncts, leafConjuncts);

            JoinComponent leaf = new JoinComponent(leafPlan, leafConjuncts);
            leafComponents.add(leaf);
        }

        return leafComponents;
    }


    /**
     * Constructs a plan tree for evaluating the specified from-clause.
     * TODO:  COMPLETE THE DOCUMENTATION
     *
     * @param fromClause the select nodes that need to be joined.
     *
     * @param conjuncts additional conjuncts that can be applied when
     *        constructing the from-clause plan.
     *
     * @param leafConjuncts this is an output-parameter.  Any conjuncts
     *        applied in this plan from the <tt>conjuncts</tt> collection
     *        should be added to this out-param.
     *
     * @return a plan tree for evaluating the specified from-clause
     *
     * @throws IOException if an IO error occurs when the planner attempts to
     *         load schema and indexing information.
     *
     * @throws IllegalArgumentException if the specified from-clause is a join
     *         expression that isn't an outer join, or has some other
     *         unrecognized type.
     */
    private PlanNode makeLeafPlan(FromClause fromClause,
        Collection<Expression> conjuncts, HashSet<Expression> leafConjuncts)
        throws IOException {

        // TODO:  IMPLEMENT.
        //        If you apply any conjuncts then make sure to add them to the
        //        leafConjuncts collection.
        //
        //        Don't forget that all from-clauses can specify an alias.
        //
        //        Concentrate on properly handling cases other than outer
        //        joins first, then focus on outer joins once you have the
        //        typical cases supported.
        if (fromClause == null) {
            return null;
        }

        // Create basic plan based on fromClause type
        PlanNode resPlan;
        if (fromClause.isBaseTable()) {
            // Use FileScanNode
            resPlan = makeSimpleSelect(fromClause.getTableName(),
                    null, null);
        }
        else if (fromClause.isDerivedTable()) {
            // Get query plan for subquery
            resPlan = makePlan(fromClause.getSelectClause(), null);
        }
        else if (fromClause.isOuterJoin()) {
            // Generate optimal plan for each child
            JoinComponent left, right;
            if (fromClause.hasOuterJoinOnLeft()) {
                left = makeJoinPlan(fromClause.getLeftChild(), null);
            }
            else {
                left = makeJoinPlan(fromClause.getLeftChild(), conjuncts);
            }
            if (fromClause.hasOuterJoinOnRight()) {
                right = makeJoinPlan(fromClause.getRightChild(), null);
            }
            else {
                right = makeJoinPlan(fromClause.getRightChild(), conjuncts);
            }
            resPlan = null;
        }
        else {
            throw new IOException("makeLeafPlan: Unknown FromClause type");
        }

        // TODO: Optimize to apply selections as early as possible
        // Note that this is not optimal in presence of indexes
        resPlan.prepare();

        return resPlan;
    }


    /**
     * This helper method builds up a full join-plan using a dynamic programming
     * approach.  The implementation maintains a collection of optimal
     * intermediate plans that join <em>n</em> of the leaf nodes, each with its
     * own associated cost, and then uses that collection to generate a new
     * collection of optimal intermediate plans that join <em>n+1</em> of the
     * leaf nodes.  This process completes when all leaf plans are joined
     * together; there will be <em>one</em> plan, and it will be the optimal
     * join plan (as far as our limited estimates can determine, anyway).
     *
     * @param leafComponents the collection of leaf join-components, generated
     *        by the {@link #generateLeafJoinComponents} method.
     *
     * @param conjuncts the collection of all conjuncts found in the query
     *
     * @return a single {@link JoinComponent} object that joins all leaf
     *         components together in an optimal way.
     */
    private JoinComponent generateOptimalJoin(
        ArrayList<JoinComponent> leafComponents, Set<Expression> conjuncts) {

        // This object maps a collection of leaf-plans (represented as a
        // hash-set) to the optimal join-plan for that collection of leaf plans.
        //
        // This collection starts out only containing the leaf plans themselves,
        // and on each iteration of the loop below, join-plans are grown by one
        // leaf.  For example:
        //   * In the first iteration, all plans joining 2 leaves are created.
        //   * In the second iteration, all plans joining 3 leaves are created.
        //   * etc.
        // At the end, the collection will contain ONE entry, which is the
        // optimal way to join all N leaves.  Go Go Gadget Dynamic Programming!
        HashMap<HashSet<PlanNode>, JoinComponent> joinPlans = new HashMap<>();

        // Initially populate joinPlans with just the N leaf plans.
        for (JoinComponent leaf : leafComponents)
            joinPlans.put(leaf.leavesUsed, leaf);

        while (joinPlans.size() > 1) {
            logger.debug("Current set of join-plans has " + joinPlans.size() +
                " plans in it.");

            // This is the set of "next plans" we will generate.  Plans only
            // get stored if they are the first plan that joins together the
            // specified leaves, or if they are better than the current plan.
            HashMap<HashSet<PlanNode>, JoinComponent> nextJoinPlans =
                new HashMap<>();

            for (JoinComponent plan : joinPlans.values()) {
                for (JoinComponent leafComponent: leafComponents) {
                    if (plan.leavesUsed.contains(leafComponent.getLeafPlan())) {
                        continue;
                    }
                    /*
                       - Make join between plan and leafComponent
                       - Calculate cost of new join
                       - if in nextJoinPlans,
                            add conditionally on costs
                       - else
                            add to nextJoinPlans
                     */
                    HashSet<Expression> tmpConjuncts = new HashSet<Expression>();
                    PredicateUtils.findExprsUsingSchemas(conjuncts, false, tmpConjuncts,
                            plan.joinPlan.getSchema(), leafComponent.joinPlan.getSchema());
                    Expression pred = PredicateUtils.makePredicate(tmpConjuncts);

                    // CROSS join or??
                    PlanNode tmpPlan = new NestedLoopJoinNode(plan.joinPlan, leafComponent.joinPlan, JoinType.CROSS, pred);
                    tmpPlan.prepare();
                    PlanCost tmpPlanCost = tmpPlan.getCost();

                    // collect leaves used
                    HashSet<PlanNode> tmpLeavesUsed = new HashSet<>();
                    tmpLeavesUsed.addAll(plan.leavesUsed);
                    tmpLeavesUsed.addAll(leafComponent.leavesUsed);

                    JoinComponent tmpJoin = new JoinComponent(tmpPlan, tmpLeavesUsed, tmpConjuncts);
                    if (nextJoinPlans.containsKey(tmpLeavesUsed)) {
                        PlanCost prevCost = nextJoinPlans.get(tmpLeavesUsed).joinPlan.getCost();
                        if (prevCost.cpuCost > tmpPlanCost.cpuCost) {
                            nextJoinPlans.put(tmpLeavesUsed, tmpJoin);
                        }
                    }
                    else {
                        nextJoinPlans.put(tmpLeavesUsed, tmpJoin);
                    }


                }
            }

            // TODO:  IMPLEMENT THE CODE THAT GENERATES OPTIMAL PLANS THAT
            //        JOIN N + 1 LEAVES

            // Now that we have generated all plans joining N leaves, time to
            // create all plans joining N + 1 leaves.
            joinPlans = nextJoinPlans;
        }

        // At this point, the set of join plans should only contain one plan,
        // and it should be the optimal plan.

        assert joinPlans.size() == 1 : "There can be only one optimal join plan!";
        return joinPlans.values().iterator().next();
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
