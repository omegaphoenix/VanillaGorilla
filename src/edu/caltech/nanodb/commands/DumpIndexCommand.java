package edu.caltech.nanodb.commands;


import java.io.IOException;

import edu.caltech.nanodb.plannodes.PlanNode;
import edu.caltech.nanodb.relations.SchemaNameException;
import edu.caltech.nanodb.storage.StorageManager;


/**
 * <p>
 * This command object represents a <tt>DUMP INDEX</tt> command issued against
 * the database.  <tt>DUMP INDEX</tt> commands are pretty simple, having a
 * single form:   <tt>DUMP INDEX ... ON TABLE ... [TO FILE ...] [FORMAT ...]</tt>.
 * </p>
 */
public class DumpIndexCommand extends DumpCommand {

    private String indexName;


    private String tableName;


    public DumpIndexCommand(String indexName, String tableName,
                            String fileName, String format) {
        super(fileName, format);

        if (indexName == null)
            throw new IllegalArgumentException("indexName cannot be null");

        if (tableName == null)
            throw new IllegalArgumentException("tableName cannot be null");

        this.indexName = indexName;
        this.tableName = tableName;

    }


    protected PlanNode prepareDumpPlan(StorageManager storageManager)
        throws IOException, SchemaNameException {

        // TODO:  Index scan!
        return null;
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
