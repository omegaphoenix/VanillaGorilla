package edu.caltech.nanodb.commands;


import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;

import org.apache.log4j.Logger;

import edu.caltech.nanodb.indexes.IndexInfo;
import edu.caltech.nanodb.indexes.IndexManager;
import edu.caltech.nanodb.indexes.IndexUtils;
import edu.caltech.nanodb.relations.TableInfo;
import edu.caltech.nanodb.relations.TableSchema;
import edu.caltech.nanodb.server.NanoDBServer;
import edu.caltech.nanodb.storage.StorageManager;
import edu.caltech.nanodb.storage.TableManager;
import edu.caltech.nanodb.storage.TupleFile;


/**
 * This Command class represents the <tt>VERIFY</tt> SQL command, which
 * verifies a table's representation (along with any indexes) to ensure that
 * all structural details are valid.  This is not a standard SQL command, but
 * it is very useful for verifying student implementations of file structures.
 */
public class VerifyCommand extends Command {
    /** A logging object for reporting anything interesting that happens. **/
    private static Logger logger = Logger.getLogger(VerifyCommand.class);


    /**
     * Table names are kept in a set so that we don't need to worry about a
     * particular table being specified multiple times.
     */
    private LinkedHashSet<String> tableNames;


    /**
     * Construct a new <tt>VERIFY</tt> command with an empty table list.
     * Tables can be added to the internal list using the {@link #addTable}
     * method.
     */
    public VerifyCommand() {
        super(Command.Type.UTILITY);
        tableNames = new LinkedHashSet<>();
    }


    /**
     * Construct a new <tt>VERIFY</tt> command to verify the specified table.
     *
     * @param tableName the name of the table to verify.
     */
    public VerifyCommand(String tableName) {
        this();
        addTable(tableName);
    }


    /**
     * Add a table to the list of tables to verify.
     *
     * @param tableName the name of the table to verify.
     */
    public void addTable(String tableName) {
        if (tableName == null)
            throw new NullPointerException("tableName cannot be null");

        tableNames.add(tableName);
    }


    @Override
    public void execute(NanoDBServer server) throws ExecutionException {

        // Make sure that all the tables are valid.

        StorageManager storageManager = server.getStorageManager();
        TableManager tableManager = storageManager.getTableManager();
        IndexManager indexManager = storageManager.getIndexManager();

        ArrayList<TableInfo> tableInfos = new ArrayList<>();

        for (String table : tableNames) {
            try {
                TableInfo tableInfo = tableManager.openTable(table);
                tableInfos.add(tableInfo);
            }
            catch (IOException ioe) {
                throw new ExecutionException("Could not open table " + table, ioe);
            }
        }

        // Now, verify each table.

        for (TableInfo tableInfo : tableInfos) {
            String tableName = tableInfo.getTableName();
            try {
                List<String> errors;
                out.println("Verifying table " + tableName);

                TupleFile tableTupleFile = tableInfo.getTupleFile();
                errors = tableTupleFile.verify();
                for (String error : errors)
                    out.println(" * " + error);

                TableSchema schema = tableInfo.getSchema();
                for (String indexName : schema.getIndexNames()) {
                    IndexInfo indexInfo = indexManager.openIndex(tableInfo, indexName);

                    out.println("\nVerifying index " + indexName +
                        " on table " + tableName);

                    TupleFile indexTupleFile = indexInfo.getTupleFile();
                    errors = IndexUtils.verifyIndex(tableTupleFile, indexTupleFile);
                    for (String error : errors)
                        out.println(" * " + error);
                }
            }
            catch (IOException ioe) {
                throw new ExecutionException("Could not update statistics on table " +
                    tableInfo.getTableName(), ioe);
            }
        }
        out.println("\nVerification complete.");
    }


    /**
     * Prints a simple representation of the verify command, including the
     * names of the tables to be verified.
     *
     * @return a string representing this verify command
     */
    @Override
    public String toString() {
        return "Verify[" + tableNames + "]";
    }
}
