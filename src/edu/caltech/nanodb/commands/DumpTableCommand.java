package edu.caltech.nanodb.commands;


import java.io.IOException;

import edu.caltech.nanodb.plannodes.PlanNode;
import edu.caltech.nanodb.queryeval.Planner;
import edu.caltech.nanodb.queryeval.PlannerFactory;
import edu.caltech.nanodb.relations.SchemaNameException;
import edu.caltech.nanodb.storage.StorageManager;


/**
 * <p>
 * This command object represents a <tt>DUMP TABLE</tt> command issued against
 * the database.  <tt>DUMP TABLE</tt> commands are pretty simple, having a
 * single form:   <tt>DUMP TABLE ... [TO FILE ...] [FORMAT ...]</tt>.
 * </p>
 * <p>
 * This command is effectively identical to <tt>SELECT * FROM tbl;</tt> with
 * the results being output to console or a data file for analysis.  The
 * command is particularly helpful when the planner only implements
 * {@link Planner#makeSimpleSelect}; no planning is needed for this command
 * to work.
 * </p>
 */
public class DumpTableCommand extends DumpCommand {

    /** The name of the table to dump. */
    private String tableName;


    /**
     * Constructs a new dump-table command.
     *
     * @param tableName the name of the table to dump
     *
     * @param fileName the path and file to dump the data to.  The console
     *        will be used if this is @code{null}.
     *
     * @param format the format to dump the data in
     *
     * @throws IllegalArgumentException if tableName is null.
     */
    public DumpTableCommand(String tableName, String fileName, String format) {
        super(fileName, format);

        if (tableName == null)
            throw new IllegalArgumentException("tableName cannot be null");

        this.tableName = tableName;
    }


    protected PlanNode prepareDumpPlan(StorageManager storageManager)
            throws IOException, SchemaNameException {

        // Create a simple plan for scanning the table.
        Planner planner = PlannerFactory.getPlanner(storageManager);
        PlanNode plan = planner.makeSimpleSelect(tableName, null, null);
        plan.prepare();

        return plan;
    }


    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();

        sb.append("DumpTableCommand[table=");
        sb.append(tableName);

        if (fileName != null) {
            sb.append(", filename=\"");
            sb.append(fileName);
            sb.append("\"");
        }

        if (format != null) {
            sb.append(", format=");
            sb.append(format);
        }

        sb.append(']');

        return sb.toString();
    }
}
