package edu.caltech.nanodb.commands;


import java.io.IOException;

import edu.caltech.nanodb.expressions.Expression;
import edu.caltech.nanodb.expressions.TupleLiteral;

import edu.caltech.nanodb.queryeval.Planner;
import edu.caltech.nanodb.queryeval.PlannerFactory;
import edu.caltech.nanodb.queryeval.TupleProcessor;

import edu.caltech.nanodb.relations.Schema;
import edu.caltech.nanodb.relations.SchemaNameException;
import edu.caltech.nanodb.relations.TableInfo;
import edu.caltech.nanodb.relations.Tuple;

import edu.caltech.nanodb.server.EventDispatcher;

import edu.caltech.nanodb.storage.TableManager;
import edu.caltech.nanodb.storage.TupleFile;
import edu.caltech.nanodb.storage.StorageManager;


/**
 * <p>
 * This command object represents a top-level <tt>DELETE</tt> command issued
 * against the database.  <tt>DELETE</tt> commands are pretty simple, having a
 * single form:   <tt>DELETE FROM ... [WHERE ...]</tt>.
 * </p>
 * <p>
 * Execution of this command is relatively straightforward; the only nuance is
 * that we can treat it as a "select for deletion", and possibly perform the
 * deletion in an optimized manner (particularly if the <tt>WHERE</tt> clause
 * contains subqueries).  Internally, we treat it as a <tt>SELECT</tt>, and
 * each row returned is deleted from the specified table.
 * </p>
 */
public class DeleteCommand extends QueryCommand {

    /**
     * An implementation of the tuple processor interface used by the
     * {@link DeleteCommand} to delete each tuple.
     */
    private static class TupleRemover implements TupleProcessor {
        /** The table whose tuples will be deleted. */
        private TableInfo tableInfo;

        private TupleFile tupleFile;

        /** The event-dispatcher singleton for firing row-delete events. */
        private EventDispatcher eventDispatcher;


        /**
         * Initialize the tuple-remover object with the details it needs to
         * delete tuples from the specified table.
         *
         * @param tableInfo details of the table that will be modified
         */
        public TupleRemover(EventDispatcher eventDispatcher,
                            TableInfo tableInfo) {
            this.tableInfo = tableInfo;
            this.tupleFile = tableInfo.getTupleFile();
            this.eventDispatcher = eventDispatcher;
        }

        /** This tuple-processor implementation doesn't care about the schema. */
        public void setSchema(Schema schema) {
            // Ignore.
        }

        /** This implementation simply deletes each tuple it is handed. */
        public void process(Tuple tuple) throws IOException {

            // Make a copy of this, because once we delete the tuple, we can't
            // use the "tuple" variable anymore!
            TupleLiteral oldTuple = new TupleLiteral(tuple);

            eventDispatcher.fireBeforeRowDeleted(tableInfo, tuple);
            tupleFile.deleteTuple(tuple);
            eventDispatcher.fireAfterRowDeleted(tableInfo, oldTuple);
        }

        public void finish() {
            // Not used
        }
    }


    /** The name of the table that the data will be deleted from. */
    private String tableName;


    /**
     * If a <tt>WHERE</tt> expression is specified, this field will refer to
     * the expression to be evaluated.
     */
    private Expression whereExpr = null;


    TableInfo tableInfo;


    /**
     * Constructs a new delete command.
     *
     * @param tableName the name of the table from which we will be deleting
     *        tuples
     *
     * @param whereExpr the predicate governing which rows will be deleted
     */
    public DeleteCommand(String tableName, Expression whereExpr) {
        super(QueryCommand.Type.DELETE);

        if (tableName == null)
            throw new NullPointerException("tableName cannot be null");

        this.tableName = tableName;
        this.whereExpr = whereExpr;
    }


    @Override
    protected void prepareQueryPlan(StorageManager storageManager)
        throws IOException, SchemaNameException {

        // Open the table and save the TableInfo so that the
        // getTupleProcessor() method can use it.
        TableManager tableManager = storageManager.getTableManager();
        tableInfo = tableManager.openTable(tableName);

        // Create a plan for executing the SQL query.
        Planner planner = PlannerFactory.getPlanner(storageManager);
        plan = planner.makeSimpleSelect(tableName, whereExpr, null);
        plan.prepare();
    }


    @Override
    protected TupleProcessor getTupleProcessor(EventDispatcher eventDispatcher) {
        return new TupleRemover(eventDispatcher, tableInfo);
    }


    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();

        sb.append("DeleteCommand[table=");

        sb.append(tableName);

        if (whereExpr != null) {
            sb.append(", whereExpr=\"");
            sb.append(whereExpr);
            sb.append("\"");
        }
        sb.append(']');

        return sb.toString();
    }
}
