package edu.caltech.nanodb.commands;


import java.io.IOException;

import org.apache.log4j.Logger;

import edu.caltech.nanodb.indexes.IndexManager;
import edu.caltech.nanodb.relations.TableInfo;
import edu.caltech.nanodb.server.NanoDBServer;
import edu.caltech.nanodb.storage.StorageManager;
import edu.caltech.nanodb.storage.TableManager;


/**
 * This command-class represents the <tt>DROP INDEX</tt> DDL command.
 */
public class DropIndexCommand extends Command {
    /** A logging object for reporting anything interesting that happens. */
    private static Logger logger = Logger.getLogger(DropIndexCommand.class);


    /** The name of the index to drop. */
    private String indexName;


    /** The name of the table that the index is built against. */
    private String tableName;


    public DropIndexCommand(String indexName, String tableName) {
        super(Type.DDL);

        if (tableName == null)
            throw new IllegalArgumentException("tableName cannot be null");

        this.indexName = indexName;
        this.tableName = tableName;
    }


    @Override
    public void execute(NanoDBServer server) throws ExecutionException {

        StorageManager storageManager = server.getStorageManager();
        TableManager tableManager = storageManager.getTableManager();
        IndexManager indexManager = storageManager.getIndexManager();

        try {
            // Open the table, then attempt to drop the index.  If it works,
            // save the table's schema back to the table file.

            TableInfo tableInfo = tableManager.openTable(tableName);
            indexManager.dropIndex(tableInfo, indexName);
            tableManager.saveTableInfo(tableInfo);
        }
        catch (IOException e) {
            throw new ExecutionException(String.format(
                "Could not drop index \"%s\" on table \"%s\".  See " +
                    "nested exception for details.", indexName, tableName), e);
        }

        out.printf("Dropped index %s on table %s.%n", indexName, tableName);
    }
}
