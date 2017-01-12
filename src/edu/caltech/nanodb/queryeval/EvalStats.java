package edu.caltech.nanodb.queryeval;


/**
 * This class holds statistics generated from a query evaluation operation,
 * such as the number of rows fetched and the total time to perform the
 * evaluation.
 */
public class EvalStats {

    /** The total rows produced by the query evaluation. */
    private int rowsProduced;


    /** The total time elapsed for query evaluation, in nanoseconds. */
    private long elapsedTimeNanos;


    public EvalStats(int rowsProduced, long elapsedTimeNanos) {
        this.rowsProduced = rowsProduced;
        this.elapsedTimeNanos = elapsedTimeNanos;
    }


    public int getRowsProduced() {
        return rowsProduced;
    }


    public long getElapsedTimeNanos() {
        return elapsedTimeNanos;
    }


    public float getElapsedTimeSecs() {
        // 1 second contains 10^9 nanoseconds
        return (float) elapsedTimeNanos / 1e9f;
    }
}
