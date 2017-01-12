package edu.caltech.nanodb.plannodes;


import edu.caltech.nanodb.expressions.BooleanOperator;
import edu.caltech.nanodb.expressions.Expression;


/**
 * A collection of helpful utilities that can be used for generating,
 * analyzing and manipulating query execution plans.
 */
public class PlanUtils {
    /**
     * This class should not be instantiated.
     */
    private PlanUtils() {
        throw new RuntimeException("This class should not be instantiated.");
    }


    /**
     * This helper function takes a query plan and a selection predicate, and
     * adds the predicate to the plan in a reasonably intelligent way.
     * <p>
     * If the plan is a subclass of the {@link SelectNode} then the select
     * node's predicate is updated to include the predicate.  Specifically, if
     * the select node already has a predicate then one of the following occurs:
     * <ul>
     *   <li>If the select node currently has no predicate, the new predicate
     *       is assigned to the select node.</li>
     *   <li>If the select node has a predicate whose top node is a
     *       {@link BooleanOperator} of type <tt>AND</tt>, this predicate is
     *       added as a new term on that node.</li>
     *   <li>If the select node has some other kind of non-<tt>null</tt>
     *       predicate then this method creates a new top-level <tt>AND</tt>
     *       operation that will combine the two predicates into one.</li>
     * </ul>
     * <p>
     * If the plan is <em>not</em> a subclass of the {@link SelectNode} then a
     * new {@link SimpleFilterNode} is added above the current plan node, with
     * the specified predicate.
     *
     * @param plan the plan to add the selection predicate to
     *
     * @param predicate the selection predicate to add to the plan
     *
     * @return the (possibly new) top plan-node for the plan with the selection
     *         predicate applied
     */
    public static PlanNode addPredicateToPlan(PlanNode plan,
                                              Expression predicate) {
        if (plan instanceof SelectNode) {
            SelectNode selectNode = (SelectNode) plan;

            if (selectNode.predicate != null) {
                // There is already an existing predicate.  Add this as a
                // conjunct to the existing predicate.
                Expression fsPred = selectNode.predicate;
                boolean handled = false;

                // If the current predicate is an AND operation, just make
                // the where-expression an additional term.
                if (fsPred instanceof BooleanOperator) {
                    BooleanOperator bool = (BooleanOperator) fsPred;
                    if (bool.getType() == BooleanOperator.Type.AND_EXPR) {
                        bool.addTerm(predicate);
                        handled = true;
                    }
                }

                if (!handled) {
                    // Oops, the current file-scan predicate wasn't an AND.
                    // Create an AND expression instead.
                    BooleanOperator bool =
                        new BooleanOperator(BooleanOperator.Type.AND_EXPR);
                    bool.addTerm(fsPred);
                    bool.addTerm(predicate);
                    selectNode.predicate = bool;
                }
            }
            else {
                // Simple - just add where-expression onto the file-scan.
                selectNode.predicate = predicate;
            }
        }
        else {
            // The subplan is more complex, so put a filter node above it.
            plan = new SimpleFilterNode(plan, predicate);
        }

        return plan;
    }
}
