package edu.caltech.nanodb.commands;


import java.io.IOException;
import java.util.List;

import edu.caltech.nanodb.expressions.Expression;
import edu.caltech.nanodb.expressions.ExpressionException;
import edu.caltech.nanodb.expressions.TupleLiteral;

import edu.caltech.nanodb.queryeval.Planner;
import edu.caltech.nanodb.queryeval.PlannerFactory;
import edu.caltech.nanodb.queryeval.TupleProcessor;
import edu.caltech.nanodb.queryast.SelectClause;
import edu.caltech.nanodb.relations.Schema;
import edu.caltech.nanodb.relations.SchemaNameException;
import edu.caltech.nanodb.relations.TableInfo;
import edu.caltech.nanodb.relations.Tuple;
import edu.caltech.nanodb.server.EventDispatcher;
import edu.caltech.nanodb.server.NanoDBServer;
import edu.caltech.nanodb.storage.TableManager;
import edu.caltech.nanodb.storage.TupleFile;
import edu.caltech.nanodb.storage.StorageManager;


/**
 * This command object represents a top-level <tt>INSERT</tt> command issued
 * against the database.  <tt>INSERT</tt> commands have two forms.  The first
 * form is <tt>INSERT ... VALUES</tt>, in which case a literal tuple-value is
 * specified and stored.  The second form is <tt>INSERT</tt> ... <tt>SELECT</tt>,
 * in which case a select-clause object is evaluated, and its results are stored
 * into the specified table.
 *
 * @see edu.caltech.nanodb.expressions.TupleLiteral
 * @see SelectClause
 */
public class InsertCommand extends QueryCommand {

    /**
     * An implementation of the tuple processor interface used by the
     * {@link InsertCommand} to insert tuples into a table, when the command is
     * of the form <tt>INSERT</tt> ... <tt>SELECT</tt>.
     */
    private static class TupleInserter implements TupleProcessor {
        /** The table into which the new tuples will be inserted. */
        private TableInfo tableInfo;

        private TupleFile tupleFile;

        /**
         * The event-dispatcher for reporting insert events to other
         * components.
         */
        EventDispatcher eventDispatcher;

        /**
         * Initialize the tuple-inserter object with the details it needs to
         * insert tuples into the specified table.
         *
         * @param tableInfo details of the table that will be modified
         */
        public TupleInserter(EventDispatcher eventDispatcher, TableInfo tableInfo) {
            this.tableInfo = tableInfo;
            this.tupleFile = tableInfo.getTupleFile();

            this.eventDispatcher = eventDispatcher;
        }

        /**
         * This implementation ignores the schema of the results, since we just
         * don't care.
         *
         * @todo We could compare the schemas to see if they are compatible...
         */
        public void setSchema(Schema schema) {
            // Ignore.
        }

        /** This implementation simply inserts each tuple it is handed. */
        public void process(Tuple tuple) throws IOException {
            eventDispatcher.fireBeforeRowInserted(tableInfo, tuple);
            Tuple newTuple = tupleFile.addTuple(tuple);
            eventDispatcher.fireAfterRowInserted(tableInfo, newTuple);
        }

        public void finish() {
            // Not used
        }
    }


    /** The name of the table that the data will be inserted into. */
    private String tableName;


    /**
     * An optional list of column-names that can be specified in the INSERT
     * command.  If the INSERT command didn't specify a list of column-names
     * then this will be <code>null</code>.
     */
    private List<String> colNames;


    /**
     * When the insert command is of the form <code>INSERT ... VALUES</code>,
     * the literal values are stored in this variable.  Otherwise this will be
     * set to <code>null</code>.
     */
    private List<Expression> values;


    /**
     * When the insert command is of the form <code>INSERT ... SELECT</code>,
     * the details of the select-clause are stored in this variable.
     * Otherwise this will be set to <code>null</code>.
     */
    private SelectClause selClause;


    private TableInfo tableInfo;


    /**
     * Constructs a new insert command for <tt>INSERT</tt> ... <tt>VALUES</tt>
     * statements.
     */
    public InsertCommand(String tableName, List<String> colNames,
        List<Expression> values) {

        super(QueryCommand.Type.INSERT);

        if (tableName == null)
            throw new NullPointerException("tableName cannot be null");

        if (values == null)
            throw new NullPointerException("values cannot be null");

        this.tableName = tableName;
        this.colNames = colNames;
        this.values = values;
    }


    /**
     * Constructs a new insert command for <tt>INSERT</tt> ... <tt>SELECT</tt>
     * statements.
     */
    public InsertCommand(String tableName, List<String> colNames,
        SelectClause selClause) {

        super(QueryCommand.Type.INSERT);

        if (tableName == null)
            throw new NullPointerException("tableName cannot be null");

        if (selClause == null)
            throw new NullPointerException("selClause cannot be null");

        this.tableName = tableName;
        this.colNames = colNames;
        this.selClause = selClause;
    }


    @Override
    public void execute(NanoDBServer server) throws ExecutionException {
        if (values != null) {
            // Inserting a single row.
            if (!explain)
                insertSingleRow(server);
            else
                out.println("Nothing to explain about INSERT ... VALUES");
        }
        else {
            // Inserting the results of a SELECT query.
            super.execute(server);
        }
    }


    /** This method is used when inserting only a single row of data. */
    private void insertSingleRow(NanoDBServer server)
        throws ExecutionException {

        StorageManager storageManager = server.getStorageManager();
        TableManager tableManager = storageManager.getTableManager();
        try {
            tableInfo = tableManager.openTable(tableName);
        }
        catch (IOException ioe) {
            throw new ExecutionException("Could not open table \"" +
                tableName + "\".", ioe);
        }

        // Build up a tuple-literal from the values we have.
        TupleLiteral tuple = new TupleLiteral();
        for (Expression expr : values) {
            if (expr.hasSymbols()) {
                throw new ExecutionException(
                    "INSERT values cannot contain symbols!");
            }

            try {
                tuple.addValue(expr.evaluate());
            }
            catch (ExpressionException e) {
                // This should be rare, but is still possible -- users
                // can type anything...
                throw new ExecutionException("Couldn't evaluate an INSERT value.", e);
            }
        }

        EventDispatcher eventDispatcher = server.getEventDispatcher();
        try {
            TupleFile tupleFile = tableInfo.getTupleFile();

            eventDispatcher.fireBeforeRowInserted(tableInfo, tuple);
            Tuple newTuple = tupleFile.addTuple(tuple);
            eventDispatcher.fireAfterRowInserted(tableInfo, newTuple);
        }
        catch (IOException e) {
            throw new ExecutionException("Couldn't insert row into table.", e);
        }
    }


    @Override
    protected void prepareQueryPlan(StorageManager storageManager)
        throws IOException, SchemaNameException {

        // Open the table and save the TableInfo so that the
        // getTupleProcessor() method can use it.
        TableManager tableManager = storageManager.getTableManager();
        tableInfo = tableManager.openTable(tableName);

        //Schema resultSchema = selClause.computeSchema();

        // Create a plan for executing the SQL query.
        Planner planner = PlannerFactory.getPlanner(storageManager);
        plan = planner.makePlan(selClause, null);
    }


    @Override
    protected TupleProcessor getTupleProcessor(EventDispatcher eventDispatcher) {
        return new TupleInserter(eventDispatcher, tableInfo);
    }


    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();

        sb.append("InsertCommand[");

        if (colNames != null) {
            sb.append("cols=(");
            boolean first = true;
            for (String colName : colNames) {
                if (first)
                    first = false;
                else
                    sb.append(',');

                sb.append(colName);
            }
            sb.append("), ");
        }

        if (values != null) {
            sb.append("values=(");
            boolean first = true;
            for (Expression e : values) {
                if (first)
                    first = false;
                else
                    sb.append(',');

                sb.append(e);
            }
            sb.append(')');
        }
        else {
            sb.append("select=");
            sb.append(selClause);
        }

        sb.append(']');

        return sb.toString();
    }
}

