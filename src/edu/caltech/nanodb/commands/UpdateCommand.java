package edu.caltech.nanodb.commands;


import java.io.IOException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import edu.caltech.nanodb.expressions.Environment;
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
 * This command object represents a top-level <tt>UPDATE</tt> command issued
 * against the database.
 */
public class UpdateCommand extends QueryCommand {

    /**
     * An implementation of the tuple processor interface used by the
     * {@link UpdateCommand} to update each tuple.
     */
    private static class TupleUpdater implements TupleProcessor {
        /** The table whose tuples will be modified. */
        private TableInfo tableInfo;

        private TupleFile tupleFile;

        /** The event-dispatcher for firing row-update events. */
        private EventDispatcher eventDispatcher;

        /**
         * This is the list of values to change in the <tt>UPDATE</tt> statement.
         */
        private List<UpdateValue> values;

        /**
         * The schema of the input tuples produced by the query evaluation.  The
         * update-expressions are evaluated in the context of this schema.
         */
        private Schema schema;

        /**
         * The environment used to evaluate the update expressions.  This object
         * is created once and reused throughout the update operation.
         */
        private Environment environment = new Environment();

        /**
         * The map containing column names and their new values for the update
         * operation.  This object is created once and reused throughout the
         * update operation.
         */
        private HashMap<String, Object> newValues = new HashMap<>();

        /**
         * Initialize the tuple-updater object with the details it needs to
         * modify tuples in the specified table.
         *
         * @param tableInfo details of the table that will be modified
         *
         * @param values the list of columns to be updated, along with the
         *        corresponding expressions to generate the new values
         */
        public TupleUpdater(EventDispatcher eventDispatcher,
                            TableInfo tableInfo, List<UpdateValue> values) {
            this.tableInfo = tableInfo;
            this.values = values;

            // Retrieve this value since we use it over and over again.
            this.tupleFile = tableInfo.getTupleFile();

            this.eventDispatcher = eventDispatcher;
        }

        /**
         * Stores the schema that will be produced during result evaluation.
         * Currently this will almost certainly be the table-file's schema, but
         * if multiple-table update support is added then this could be an
         * aggregated schema.
         */
        public void setSchema(Schema schema) {
            this.schema = schema;
        }

        /**
         * This implementation updates each tuple it is handed, based on the
         * set of update-specs that were given in the constructor.
         */
        public void process(Tuple tuple) throws IOException {

            // Figure out what the new values should be for the tuple we are
            // processing.

            environment.clear();
            environment.addTuple(schema, tuple);

            // Make copies of the old and new tuple-values so that we can pass
            // them to the before- and after-update handlers.
            TupleLiteral oldTuple = new TupleLiteral(tuple);
            TupleLiteral newTuple = new TupleLiteral(tuple);

            newValues.clear();
            for (UpdateValue value : values) {
                Object result = value.getExpression().evaluate(environment);
                newValues.put(value.getColumnName(), result);

                // TODO:  Should probably coerce to the specific column type
                //        specified in the table's schema...
                newTuple.setColumnValue(
                    schema.getColumnIndex(value.getColumnName()), result);
            }

            eventDispatcher.fireBeforeRowUpdated(tableInfo, tuple, newTuple);
            tupleFile.updateTuple(tuple, newValues);
            eventDispatcher.fireAfterRowUpdated(tableInfo, oldTuple, tuple);
        }

        public void finish() {
            // Not used
        }
    }


    /** The name of the table that the rows will be updated on. */
    private String tableName;


    /**
     * This field holds the list of names and expressions that will be applied
     * in the update command.
     */
    private List<UpdateValue> values = new ArrayList<>();


    /**
     * If a <tt>WHERE</tt> expression is specified, this field will refer to
     * the expression to be evaluated.
     */
    private Expression whereExpr = null;


    private TableInfo tableInfo;


    /**
     * Constructs a new update command for <tt>UPDATE</tt> statements.
     * Note that the list of value and the where-expression are both
     * uninitialized at first.
     *
     * @param tableName the name of the table that this command will update
     */
    public UpdateCommand(String tableName) {
        super(QueryCommand.Type.UPDATE);

        if (tableName == null)
            throw new NullPointerException("tableName cannot be null");

        this.tableName = tableName;
    }


    public void addValue(String colName, Expression valueExpr) {
        values.add(new UpdateValue(colName, valueExpr));
    }


    /**
     * Sets the expression for the <tt>WHERE</tt> clause.  A <code>null</code>
     * value indicates that the UPDATE command has no WHERE condition.
     *
     * @param whereExpr the where-expression that constrains which rows will be
     *        updated
     */
    public void setWhereExpr(Expression whereExpr) {
        this.whereExpr = whereExpr;
    }


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
        return new TupleUpdater(eventDispatcher, tableInfo, values);
    }


    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();

        sb.append("UpdateCommand[table = ");
        sb.append(tableName);

        sb.append(", values = (");
        boolean first = true;
        for (UpdateValue v : values) {
            if (first)
                first = false;
            else
                sb.append(',');

            sb.append(v);
        }
        sb.append(')');

        if (whereExpr != null) {
            sb.append(", whereExpr = \"");
            sb.append(whereExpr);
            sb.append("\"");
        }

        sb.append(']');

        return sb.toString();
    }
}
