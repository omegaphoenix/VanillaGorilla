package edu.caltech.nanodb.plannodes;


import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;

import java.util.ArrayList;
import java.util.List;

import edu.caltech.nanodb.expressions.Environment;
import edu.caltech.nanodb.expressions.OrderByExpression;

import edu.caltech.nanodb.queryeval.ColumnStats;
import edu.caltech.nanodb.queryeval.PlanCost;

import edu.caltech.nanodb.relations.Schema;
import edu.caltech.nanodb.relations.Tuple;


/**
 * Represents a query plan node in its most abstract form.  To create actual
 * plan nodes, use the subclasses of this node.
 */
public abstract class PlanNode implements Cloneable {

    /** Node type enumeration. */
    public enum OperationType {
        /** Relational algebra Select operator. */
        SELECT,

        /** Relational algebra Project operator. */
        PROJECT,

        /** Relational algebra Rename operator. */
        RENAME,

        /** Relational algebra Theta Join operator. **/
        THETA_JOIN,

        /** Relational algebra Group By / Aggregate operator. */
        GROUP_AGGREGATE,

        /** Sorting operator. */
        SORT,

        /** A materialize plan-node. */
        MATERIALIZE
    }


    /** The type of this plan node. */
    protected OperationType nodeType;


    /**
     * The left child of this plan node.  If the plan node only has one child,
     * use this field.
     */
    protected PlanNode leftChild;


    /**
     * If the plan node has two children, this field is set to the right child
     * node.  If the plan has only one child, this field will be <tt>null</tt>.
     */
    protected PlanNode rightChild;


    /**
     * The schema of the results produced by this plan-node.  The schema is
     * initialized by the {@link #prepare} method; before that method has been
     * called, this will be <tt>null</tt>.
     */
    protected Schema schema;


    /**
     * The estimated cost of executing this plan and its subplans.  This cost is
     * computed by the {@link #prepare} method; before that method has been
     * called, this will be <tt>null</tt>.
     */
    protected PlanCost cost;


    /**
     * Statistics (possibly estimated) describing the results produced by this
     * plan node.  These statistics are produced when the {@link #prepare}
     * method is called; before that method has been called, this will be
     * <tt>null</tt>.
     * <p>
     * This collection corresponds to the {@link #schema} object; it will
     * contain the same number of columns, and the individual columns will
     * correspond with the specific columns in the schema object.
     */
    protected ArrayList<ColumnStats> stats;


    /**
     * The environment used to evaluate expressions against tuples being
     * processed.
     */
    protected Environment environment = new Environment();


    /**
     * Constructs a PlanNode with a given operation type.  This method will be
     * called by subclass constructors.
     *
     * @param op the operation type of the node.
     *
     * @throws IllegalArgumentException if <tt>op</tt> is <tt>null</tt>
     */
    protected PlanNode(OperationType op) {
        if (op == null)
            throw new IllegalArgumentException("op cannot be null");

        nodeType = op;
    }


    /**
     * Constructs a PlanNode with a given operation type, and the specified left
     * child-plan.  This method will be called by subclass constructors.
     *
     * @param op the operation type of the node.
     * @param leftChild the left subplan of this node.
     *
     * @throws IllegalArgumentException if <tt>op</tt> or <tt>leftChild</tt> is
     *         <tt>null</tt>
     */
    protected PlanNode(OperationType op, PlanNode leftChild) {
        this(op);

        if (leftChild == null)
            throw new IllegalArgumentException("leftChild cannot be null");

        this.leftChild = leftChild;
    }


    /**
     * Constructs a PlanNode with a given operation type, and the specified left
     * child-plan.  This method will be called by subclass constructors.
     *
     * @param op the operation type of the node.
     * @param leftChild the left subplan of this node.
     * @param rightChild the right subplan of this node.
     *
     * @throws IllegalArgumentException if <tt>op</tt>, <tt>leftChild</tt>, or
     *         <tt>rightChild</tt> is <tt>null</tt>
     */
    protected PlanNode(OperationType op, PlanNode leftChild, PlanNode rightChild) {
        this(op, leftChild);

        if (rightChild == null)
            throw new IllegalArgumentException("rightChild cannot be null");

        this.rightChild = rightChild;
    }


    /**
     * If the results are ordered in some way, this method returns a collection
     * of expressions specifying what columns or expressions the results are
     * ordered by.  If the results are not ordered then this method may return
     * either an empty list or a <tt>null</tt> value.
     * <p>
     * When this method returns a list of ordering expressions, the order of the
     * expressions themselves also matters.  The entire set of results will be
     * ordered by the first expression; rows with the same value for the first
     * expression will be ordered by the second expression; etc.
     *
     * @return If the plan node produces ordered results, this will be a list
     *         of objects specifying the ordering.  If the node doesn't produce
     *         ordered results then the return-value will either be an empty
     *         list or it will be <tt>null</tt>.
     */
    public abstract List<OrderByExpression> resultsOrderedBy();


    /**
     * This method reports whether this plan node supports marking a certain
     * point in the tuple-stream so that processing can return to that point
     * as needed.
     *
     * @return true if the node supports position marking, false otherwise.
     */
    public abstract boolean supportsMarking();


    /**
     * This method reports whether this plan node requires the left child to
     * support marking for proper evaluation.
     *
     * @return true if the node requires that its left child supports marking,
     *         false otherwise.
     */
    public abstract boolean requiresLeftMarking();


    /**
     * This method reports whether this plan node requires the right child to
     * support marking for proper evaluation.
     *
     * @return true if the node requires that its right child supports marking,
     *         false otherwise.
     */
    public abstract boolean requiresRightMarking();


    /**
     * This method is responsible for computing critical details about the plan
     * node, such as the schema of the results that are produced, the estimated
     * cost of evaluating the plan node (and its children), and statistics
     * describing the results produced by the plan node.
     */
    public abstract void prepare();


    /**
     * Returns the schema of the results that this node produces.  Some nodes
     * such as Select will not change the input schema but others, such as
     * Project, Rename, and ThetaJoin, must change it.
     * <p>
     * The schema is not computed until the {@link #prepare} method is called;
     * until that point, this method will return <tt>null</tt>.
     *
     * @return the schema produced by this plan-node.
     */
    public final Schema getSchema() {
        return schema;
    }


    /**
     * Returns the estimated cost of this plan node's operation.  The estimate
     * depends on which algorithm the node uses and the data it is working with.
     * <p>
     * The cost is not computed until the {@link #prepare} method is called;
     * until that point, this method will return <tt>null</tt>.
     *
     * @return an object containing various cost measures such as the worst-case
     *         number of disk accesses, the number of tuples produced, etc.
     */
    public final PlanCost getCost() {
        return cost;
    }


    /**
     * Returns statistics (possibly estimated) describing the results that this
     * plan node will produce.  Estimating statistics for output results is a
     * very imprecise task, to say the least.
     * <p>
     * These statistics are not computed until the {@link #prepare} method is
     * called; until that point, this method will return <tt>null</tt>.
     *
     * @return statistics describing the results that will be produced by this
     *         plan-node.
     */
    public final ArrayList<ColumnStats> getStats() {
        return stats;
    }


    /**
     * Returns the environment that this plan node uses during query
     * evaluation.
     *
     * @return the environment this plan node uses during evaluation.
     */
    public Environment getEnvironment() {
        return environment;
    }



    /**
     * Sets the plan node's environment to the specified environment object.
     * This can be used to connect subqueries to their parent environment,
     * among other things.
     *
     * @param env the environment for this plan node to use during evaluation.
     */
    public void setEnvironment(Environment env) {
        if (env == null)
            throw new IllegalArgumentException("env cannot be null");

        environment = env;
    }


    /**
     * This method adds a parent environment to the entire plan tree rooted at
     * this plan node.  This allows an execution plan to be used as a subquery
     * that references an outer plan's tuples during evaluation.
     *
     * @param parentEnv the parent environment to add to this plan tree.
     */
    public void addParentEnvironmentToPlanTree(Environment parentEnv) {
        if (parentEnv == null)
            throw new IllegalArgumentException("parentEnv cannot be null");

        environment.addParentEnvironment(parentEnv);

        if (leftChild != null)
            leftChild.addParentEnvironmentToPlanTree(parentEnv);

        if (rightChild != null)
            rightChild.addParentEnvironmentToPlanTree(parentEnv);
    }


    /**
     * Does any initialization the node might need, including setting up the
     * node's execution environment.  Subclasses of {@code PlanNode} can
     * extend this method to perform other kinds of initialization, but they
     * must be sure to call their parent class' implementation as well.
     */
    public void initialize() {
        // Do nothing.
    }


    /**
     * Gets the next tuple that fulfills the conditions for this plan node.
     * If the node has a child, it should call getNextTuple() on the child.
     * If the node is a leaf, the tuple comes from some external source such
     * as a table file, the network, etc.
     *
     * @return the next tuple to be generated by this plan, or <tt>null</tt>
     *         if the plan has finished generating plan nodes.
     *
     * @throws IOException if table data cannot be read from the filesystem
     * @throws IllegalStateException if a plan node is not properly initialized
     */
    public abstract Tuple getNextTuple()
        throws IllegalStateException, IOException;


    /**
     * Marks the current tuple in the tuple-stream produced by this node.  The
     * {@link #resetToLastMark} method can be used to return to this tuple.
     * Note that only one marker can be set in the tuple-stream at a time.
     *
     * @throws UnsupportedOperationException if the node does not support marking.
     *
     * @throws IllegalStateException if there is no "current tuple" to mark.
     *         This will occur if {@link #getNextTuple} hasn't yet been called
     *         (i.e. we are before the first tuple in the tuple-stream), or if
     *         we have already reached the end of the tuple-stream (i.e. we are
     *         after the last tuple in the stream).
     */
    public abstract void markCurrentPosition();


    /**
     * Resets the node's tuple-stream to the most recently marked position.
     * Note that only one marker can be set in the tuple-stream at a time.
     *
     * @throws UnsupportedOperationException if the node does not support marking.
     *
     * @throws IllegalStateException if {@link #markCurrentPosition} hasn't yet
     *         been called on this plan-node
     */
    public abstract void resetToLastMark();


    /**
     * Perform any necessary clean up tasks. This should probably be called
     * when we are done with this plan node.
     */
    public abstract void cleanUp();


    /**
     * Reports this node and its vital parameters as a string.
     *
     * @return the node in string format.
     *
     * @design We re-declare this here to force its implementation in subclasses.
     */
    @Override
    public abstract String toString();


    /**
     * Prints the entire plan subtree starting at this node.
     *
     * @param out PrintStream to be used for output.
     * @param includeCosts a flag controlling whether the cost estimates will be
     *        included in the plan output
     * @param indent the indentation level.
     */
    public void printNodeTree(PrintStream out, boolean includeCosts, String indent) {
        StringBuilder buf = new StringBuilder();
        buf.append(indent);
        buf.append(toString());
        if (includeCosts) {
            if (cost != null)
                buf.append(" cost=").append(cost);
            else
                buf.append(" cost is unknown");
        }

        out.println(buf.toString());

        if (leftChild != null)
            leftChild.printNodeTree(out, includeCosts, indent + "    ");

        if (rightChild != null)
            rightChild.printNodeTree(out, includeCosts, indent + "    ");
    }


    /**
     * Prints the entire node tree with indentation to the specified output
     * stream starting from indentation level 0.
     *
     * @param out the output stream.
     * @param includeCosts a flag controlling whether costs are included in the
     *        output
     */
    public void printNodeTree(PrintStream out, boolean includeCosts) {
        printNodeTree(out, includeCosts, "");
    }


    /**
     * Prints the entire node tree with indentation to the specified output
     * stream starting from indentation level 0.  Costs are not included in the
     * output.
     *
     * @param out the output stream.
     */
    public void printNodeTree(PrintStream out) {
        printNodeTree(out, false);
    }


    /**
     * Generates the same result as {@link #printNodeTree(PrintStream)}, but
     * into a string instead of to an output stream.
     *
     * @param plan The plan tree to print to a string.
     * @param includeCosts a flag controlling whether costs are included in the
     *        output
     *
     * @return A string containing the indented printout of the plan.
     */
    public static String printNodeTreeToString(PlanNode plan,
                                               boolean includeCosts) {

        // The character-encoding "US-ASCII" is defined to be supported
        // on all Java implementations, so this should never throw.
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            PrintStream ps = new PrintStream(baos, false, "US-ASCII");
            plan.printNodeTree(ps, includeCosts);
            ps.flush();

            return baos.toString("US-ASCII");
        }
        catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
    }


    /**
     * Generates the same result as {@link #printNodeTree(PrintStream)}, but
     * into a string instead of to an output stream.
     *
     * @param plan The plan tree to print to a string.
     *
     * @return A string containing the indented printout of the plan.
     */
    public static String printNodeTreeToString(PlanNode plan) {
        return printNodeTreeToString(plan, false);
    }


    /**
     * Checks if the argument is a plan node tree with the same structure,
     * but not necesarily the same references.
     *
     * @param obj the object to which we are comparing
     *
     * @design We re-declare this here to force its implementation in subclasses.
     */
    @Override
    public abstract boolean equals(Object obj);


    /**
     * Computes the hash-code of a plan-node, including any sub-plans of this
     * plan.  This method is used to see if two plan nodes (or subtrees)
     * <em>might be</em> equal.
     *
     * @return the hash code for the plan node and any subnodes it may contain.
     */
    @Override
    public abstract int hashCode();


    /**
     * Creates a copy of this plan node and its subtree.  Note that this method
     * is only used internally, because plan nodes also reference their parent
     * node, and it is not possible for this method to set that parent-reference
     * properly.  To create a deep copy of an entire plan tree, the
     * {@link #duplicate} method should be used.
     */
    @Override
    protected PlanNode clone() throws CloneNotSupportedException {
        PlanNode node = (PlanNode) super.clone();

        // NodeType is immutable.
        node.nodeType = this.nodeType;

        // Environment is used for execution.
        node.environment = new Environment();

        // Copy the children if applicable.
        if (this.leftChild != null)
            node.leftChild = this.leftChild.clone();
        else
            node.leftChild = null;

        if (this.rightChild != null)
            node.rightChild = this.rightChild.clone();
        else
            node.rightChild = null;

        // PARENT CANNOT BE COPIED, SET TO NULL AND NOTE THAT WE NEED TO
        // RUN THROUGH THE TREE AT THE END TO SET THE PARENT!

        return node;
    }


    /**
     * Iterates through the tree and sets node parents to correct references.
     * This method should be called after cloning an entire tree because cloning
     * cannot correctly clone parent references.
     **/
    private void setNodeParents() {
        if (leftChild != null) {
            leftChild.setNodeParents();
        }
        if (rightChild != null) {
            rightChild.setNodeParents();
        }
    }


    /**
     * Returns a deep copy of this plan tree.
     *
     * @return a root to the duplicated plan tree.
     */
    public PlanNode duplicate() {
        PlanNode dup = null;
        try {
            dup = clone();

            // We have successfully cloned the node tree.
            // Set the top node's parent to null and set all the other parents.
            dup.setNodeParents();
        }
        catch (CloneNotSupportedException e) {
            e.printStackTrace();
        }

        return dup;
    }
}
