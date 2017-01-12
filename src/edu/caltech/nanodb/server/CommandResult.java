package edu.caltech.nanodb.server;


import edu.caltech.nanodb.commands.SelectCommand;
import edu.caltech.nanodb.expressions.TupleLiteral;
import edu.caltech.nanodb.queryeval.TupleProcessor;
import edu.caltech.nanodb.relations.Schema;
import edu.caltech.nanodb.relations.TableSchema;
import edu.caltech.nanodb.relations.Tuple;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;


/**
 *
 * @review (Donnie) Maybe we should include the command that was run.
 */
public class CommandResult implements Serializable {

    /**
     * This inner class is used to collect the results of a
     * {@link edu.caltech.nanodb.commands.SelectCommand}.
     */
    private class ResultCollector implements TupleProcessor {
        public void setSchema(Schema s) {
            if (s instanceof TableSchema)
                s = new Schema(s);

            schema = s;
        }

        public void process(Tuple tuple) {
            // Store the tuple.
            if (tuple instanceof TupleLiteral)
                tuples.add((TupleLiteral) tuple);
            else
                tuples.add(new TupleLiteral(tuple));
        }

        public void finish() {
            // Not used
        }
    }


    /** The system time when command execution started. */
    private long startTimestamp = -1;


    /** The system time when command execution ended. */
    private long endTimestamp = -1;


    /**
     * If a failure occurs while a command is being executed, this will be the
     * exception that indicated the failure.
     */
    private Exception failure = null;



    /** The time to the first result being produced. */
    private long firstResultTimestamp = -1;


    /**
     * If the command was a <tt>SELECT</tt> query and the results are to be
     * kept, this will be the schema of the results as computed by the database.
     */
    private Schema schema = null;


    /**
     * If the command was a <tt>SELECT</tt> query and the results are to be
     * kept, this will be a collection of the tuples in the order they were
     * produced by the database.
     */
    private ArrayList<TupleLiteral> tuples = null;


    public void startExecution() {
        startTimestamp = System.currentTimeMillis();
    }


    public void collectSelectResults(SelectCommand command) {
        tuples = new ArrayList<TupleLiteral>();
        command.setTupleProcessor(new ResultCollector());
    }


    public void recordFailure(Exception e) {
        if (e == null)
            throw new IllegalArgumentException("t cannot be null");

        failure = e;
    }


    public boolean failed() {
        return (failure != null);
    }


    public Exception getFailure() {
        return failure;
    }


    public void endExecution() {
        endTimestamp = System.currentTimeMillis();
    }


    /**
     * Returns the total execution time of the command in milliseconds.
     *
     * @return the total execution time of the command in milliseconds.
     */
    public long getTotalTime() {
        return endTimestamp - startTimestamp;
    }


    /**
     * Returns the time to the first result in milliseconds.
     *
     * @return the time to the first result in milliseconds.
     */
    public long getTimeToFirstResult() {
        if (tuples == null)
            throw new IllegalStateException("The command produced no results.");

        return firstResultTimestamp - startTimestamp;
    }


    public Schema getSchema() {
        return schema;
    }


    public List<TupleLiteral> getTuples() {
        return tuples;
    }
}
