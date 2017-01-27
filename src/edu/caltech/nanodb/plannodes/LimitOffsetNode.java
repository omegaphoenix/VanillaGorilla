package edu.caltech.nanodb.plannodes;

import java.util.*;
import java.io.IOException;

import edu.caltech.nanodb.relations.Tuple;

import edu.caltech.nanodb.expressions.OrderByExpression;
import org.apache.log4j.Logger;



/**
 * PlanNode representing the <tt>LIMIT</tt> and <tt>OFFSET</tt> clauses in a
 * SQL query. This is the relational algebra Project operator.
 */
public class LimitOffsetNode extends PlanNode {

    /** A logging object for reporting anything interesting that happens. **/
    private static Logger logger = Logger.getLogger(ProjectNode.class);

    private int offsetCount = 0;
    private int limitCount = 0;
    private int offset;
    private int limit;

    private Tuple currentTuple;

    public LimitOffsetNode(PlanNode leftChild, int offset, int limit) {
        super(OperationType.LIMITOFFSET, leftChild);
        this.offset = offset;
        this.limit = limit;
    }

    public void setLimit(int limit) {
        this.limit = limit;
    }

    public void setOffset(int offset) {
        this.offset = offset;
    }


    /** Determines whether the results of the node are sorted. */

    public List<OrderByExpression> resultsOrderedBy() {
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

    public void prepare(){
        leftChild.prepare();
        schema = leftChild.getSchema();
        cost = leftChild.getCost();
        stats = leftChild.getStats();
    }

    public Tuple getNextTuple() throws IOException {
        while (offsetCount < offset) {
            advanceCurrentTuple();
            offsetCount++;
        }
        if (limit == 0 || limitCount < limit) {
            limitCount++;
            advanceCurrentTuple();
            return currentTuple;
        }
        return null;
    }

    private void advanceCurrentTuple() throws IOException {
        currentTuple = leftChild.getNextTuple();
    }


    /** The sort plan-node doesn't support marking. */
    public void markCurrentPosition() {
        throw new UnsupportedOperationException(
                "LimitOffset plan-node doesn't support marking.");
    }


    /** The sort plan-node doesn't support marking. */
    public void resetToLastMark() {
        throw new UnsupportedOperationException(
                "Sort plan-node doesn't support marking.");
    }

    public void cleanUp(){
        leftChild.cleanUp();
    }


    public String toString(){
        return String.format("LimitOffset[limit: %d, offset: %d] ", limit, offset);
    }

    public boolean equals(Object obj){
        if (obj instanceof LimitOffsetNode) {
            LimitOffsetNode other = (LimitOffsetNode) leftChild;
            if (leftChild.equals(other.leftChild)) {
                return offset == other.offset && limit == other.limit;
            }
        }
        return false;
    }

    public int hashCode(){
        int hash = 17;
        hash = 31 * hash + leftChild.hashCode() + 23 * limit + 29 * offset;
        return hash;
    }

}
