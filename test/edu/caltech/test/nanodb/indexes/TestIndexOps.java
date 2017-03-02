package edu.caltech.test.nanodb.indexes;


import java.util.Set;

import edu.caltech.nanodb.indexes.IndexManager;
import edu.caltech.nanodb.relations.TableInfo;
import edu.caltech.nanodb.storage.TableManager;

import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import edu.caltech.test.nanodb.sql.SqlTestCase;
import edu.caltech.nanodb.server.CommandResult;
import edu.caltech.nanodb.server.NanoDBServer;
import edu.caltech.nanodb.storage.StorageManager;
import edu.caltech.nanodb.relations.TableSchema;
import edu.caltech.nanodb.indexes.IndexInfo;

/**
 * This class exercises the database with some simple CREATE and DROP
 * index statements against a single table, to see if creation and deletion
 * of indexes behaves properly
 */

@Test
public class TestIndexOps extends SqlTestCase {

    public TestIndexOps() {
        super("setup_testIndexOps");
    }


    /**
     * This test simply checks if the CREATE INDEX command works properly.
     * It does so by running the command on a table, and checking various
     * signs that the index was made, such as making sure that the
     * index file exists and can be opened.
     *
     * @throws Exception if any issues occur.
     */
    public void testCreateNormalIndex() throws Throwable {
        // Perform the result to create the index. Table is test_index_ops
        server.doCommand("CREATE INDEX idx_test1 ON test_index_ops (a)", false);

        // Get the IndexInfo corresponding to the created index

        StorageManager storageManager = server.getStorageManager();
	    TableManager tableManager = storageManager.getTableManager();
        IndexManager indexManager = storageManager.getIndexManager();

        TableInfo tableInfo = tableManager.openTable("TEST_INDEX_OPS");
        IndexInfo indexInfo = indexManager.openIndex(tableInfo, "IDX_TEST1");

        // Check that the index criteria are appropriate
        assert(indexInfo.getIndexName().equals("IDX_TEST1"));
        assert(indexInfo.getTableName().equals("TEST_INDEX_OPS"));

        // Check tblFileInfo has the schemas stored
        TableSchema schema = tableInfo.getSchema();

        Set<String> indexNames = schema.getIndexNames();
        assert(indexNames.contains("IDX_TEST1"));
    }


    /**
     * This test checks to make sure that nanodb throws an error if
     * the user tries to create an index on a column that already has
     * an index.
     *
     * @throws Exception if any issues occur.
     */
    public void testCreateSameColIndex() throws Throwable {
        // Perform the result to create the index. Table is test_index_ops
        CommandResult result;
        result = server.doCommand(
                "CREATE INDEX idx_test2 ON test_index_ops (a, a)", false);
        if (!result.failed()) {
            assert false;
        }
    }


   	/**
     * This test checks to make sure that nanodb throws an error if
     * the user tries to create an index with the same name as
     * another index.
     *
     * @throws Exception if any issues occur.
     */
    public void testCreateSameIndex() throws Throwable {
        // Perform the result to create the index. Table is test_index_ops
        CommandResult result;
        result = server.doCommand(
            "CREATE INDEX idx_test3 ON test_index_ops (b)", false);
        result = server.doCommand(
            "CREATE INDEX idx_test3 ON test_index_ops (b)", false);
        if (!result.failed()) {
            // Drop the original index
            StorageManager storageManager = server.getStorageManager();
	        TableManager tableManager = storageManager.getTableManager();
            IndexManager indexManager = storageManager.getIndexManager();

            TableInfo tableInfo = tableManager.openTable("TEST_INDEX_OPS");
            indexManager.dropIndex(tableInfo, "IDX_TEST3");
            tableManager.closeTable(tableInfo);

            assert false;
        }
    }


	 /**
     * This test makes sure that the DROP INDEX command works properly.
     * In particular it runs the create index command, and then the
     * drop index command, and checks to see that the table schema
     * no longer has that index.
     *
     * @throws Exception if any issues occur.
     */
    public void testDropNormalIndex() throws Throwable {
        // Perform the result to create the index. Table is test_index_ops
        CommandResult result;
        result = server.doCommand(
            "CREATE INDEX idx_test4 ON test_index_ops (c)", false);
        result = server.doCommand(
            "DROP INDEX idx_test4 ON test_index_ops", false);
        StorageManager storageManager = server.getStorageManager();
        TableManager tableManager = storageManager.getTableManager();
        TableInfo tableInfo = tableManager.openTable("TEST_INDEX_OPS");
        // Check tblFileInfo has the index deleted from it
        TableSchema schema = tableInfo.getSchema();
        if (!schema.getIndexes().isEmpty()) {
            // Should not be any indices in here, so assert false
            assert false;
        }
    }

}
