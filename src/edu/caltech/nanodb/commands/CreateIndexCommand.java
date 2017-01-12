package edu.caltech.nanodb.commands;


import java.io.FileNotFoundException;
import java.io.IOException;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import edu.caltech.nanodb.indexes.IndexManager;

import edu.caltech.nanodb.relations.ColumnRefs;
import edu.caltech.nanodb.relations.KeyColumnRefs;
import edu.caltech.nanodb.relations.TableConstraintType;
import edu.caltech.nanodb.relations.TableInfo;

import edu.caltech.nanodb.server.NanoDBServer;
import edu.caltech.nanodb.storage.TableManager;
import edu.caltech.nanodb.storage.StorageManager;


/** This command-class represents the <tt>CREATE INDEX</tt> DDL command. */
public class CreateIndexCommand extends Command {
    /** A logging object for reporting anything interesting that happens. **/
    private static Logger logger = Logger.getLogger(CreateIndexCommand.class);


    /** The name of the index being created. */
    private String indexName;


    /**
     * This flag specifies whether the index is a unique index or not.  If the
     * value is true then no key-value may appear multiple times; if the value
     * is false then a key-value may appear multiple times.
     */
    private boolean unique;


    /** The name of the table that the index is built against. */
    private String tableName;


    /**
     * The list of column-names that the index is built against.  The order of
     * these values is important; for ordered indexes, the index records must be
     * kept in the order specified by the sequence of column names.
     */
    private ArrayList<String> columnNames = new ArrayList<>();


    /** Any additional properties specified in the command. */
    private CommandProperties properties;


    public CreateIndexCommand(String indexName, String tableName,
                              boolean unique) {
        super(Type.DDL);

        if (tableName == null)
            throw new IllegalArgumentException("tableName cannot be null");

        this.indexName = indexName;
        this.tableName = tableName;
        this.unique = unique;
    }


    public boolean isUnique() {
        return unique;
    }


    public void setProperties(CommandProperties properties) {
        this.properties = properties;
    }


    public CommandProperties getProperties() {
        return properties;
    }


    public void addColumn(String columnName) {
        this.columnNames.add(columnName);
    }

    public void addColumns(List<String> columnNames) {
        this.columnNames.addAll(columnNames);
    }


    @Override
    public void execute(NanoDBServer server) throws ExecutionException {

        StorageManager storageManager = server.getStorageManager();
        TableManager tableManager = storageManager.getTableManager();
        IndexManager indexManager = storageManager.getIndexManager();

        // Open the table and get the schema for the table.
        logger.debug(String.format("Opening table %s to retrieve schema",
            tableName));
        TableInfo tableInfo;
        try {
            tableInfo = tableManager.openTable(tableName);
        } catch (FileNotFoundException e) {
            throw new ExecutionException(String.format(
                "Specified table %s doesn't exist!", tableName), e);
        } catch (IOException e) {
            throw new ExecutionException(String.format(
                "Error occurred while opening table %s", tableName), e);
        }

        try {
            int[] cols = tableInfo.getSchema().getColumnIndexes(columnNames);

            ColumnRefs colRefs;
            if (unique) {
                colRefs = new KeyColumnRefs(indexName, cols,
                    TableConstraintType.UNIQUE);
            }
            else {
                colRefs = new ColumnRefs(indexName, cols);
            }

            indexManager.addIndexToTable(tableInfo, colRefs);
        }
        catch (IOException e) {
            throw new ExecutionException(String.format(
                "Error occurred while creating index %s on table %s",
                indexName, tableName), e);
        }

        logger.debug(String.format("New index %s on table %s is created!",
            indexName, tableName));

        out.printf("Created index %s on table %s.%n", indexName, tableName);
    }
}
