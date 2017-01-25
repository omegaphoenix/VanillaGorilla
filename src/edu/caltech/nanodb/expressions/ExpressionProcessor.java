package edu.caltech.nanodb.expressions;


/**
 * This interface is used to implement scans or transformations of expression
 * trees by specifying what to do when entering or leaving each expression
 * node.  When leaving an expression node, a replacement expression may be
 * returned that will replace the expression node that was just left.
 */
public interface ExpressionProcessor {
    /**
     * This method is called when expression-traversal is entering a
     * particular node in the expression tree.  It is not possible to replace
     * a node when entering it, because this would unnecessarily complicate
     * the semantics of expression-tree traversal.
     *
     * @param node the {@code Expression} node being entered
     */
    void enter(Expression node);


    /**
     * This method is called when expression-traversal is leaving a particular
     * node in the expression tree.  To facilitate mutation of expression
     * trees, this method must return an {@code Expression} object:  If the
     * expression processor wants to replace the node being left with a
     * different node, this method can return the replacement node; otherwise,
     * the method should return the passed-in node.
     *
     * @param node the {@code Expression} node being left
     *
     * @return the {@code Expression} object to use for the node being left;
     *         either {@code node} if no changes are to be made, or a new
     *         {@code Expression} object if {@code node} should be replaced.
     */
    Expression leave(Expression node);
}
