package edu.caltech.test.nanodb.indexes;


import edu.caltech.nanodb.relations.TableInfo;
import edu.caltech.nanodb.storage.TableManager;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import edu.caltech.test.nanodb.sql.SqlTestCase;
import edu.caltech.nanodb.server.CommandResult;
import edu.caltech.nanodb.server.NanoDBServer;
import edu.caltech.nanodb.storage.StorageManager;
import edu.caltech.nanodb.relations.TableSchema;

/**
 * This class exercises the database with some simple INSERT statements
 * on a table with UNIQUE constraints, to see if the UNIQUE constraint
 * works propertly.
 */

@Test
public class TestUniqueOps extends SqlTestCase {

    public TestUniqueOps() {
        super("setup_testUniqueOps");
    }


    /**
     * This test checks that an index with a unique constraint was created
     * when creating the table test_unique_ops on column a. It does this by
     * attempting to create an index on a again, and that should fail if the
     * index exists.
     *
     * @throws Exception if any issues occur.
     * /

    TODO:  THIS TEST WAS DISABLED BECAUSE WE CAN'T JUST CHECK IF THE COLUMNS
           ARE IN ANOTHER INDEX; MUST ALSO SEE IF THE OTHER INDEX IS THE SAME
           TYPE.  SOOO, FOR NOW WE DISABLE THIS TEST.

           SEE BasicIndexManager.addIndexToTable() FOR MORE DETAILS.

    public void testUniqueIndexExists() throws Throwable {
        // Perform the result to create the index. Table is test_unique_ops
        CommandResult result;
        result = server.doCommand(
            "CREATE UNIQUE INDEX idx_test1 ON test_unique_ops (a)", false);
        if (!result.failed()) {
            result = server.doCommand(
                "DROP INDEX idx_test1 ON test_unique_ops", false);
            assert false;
        }
    }
    */


   	/**
     * This test checks that the unique constraint added to column a is
     * enforced. It does this by trying to insert a duplicate value in
     * column a, and that the insert command should fail if the unique
     * constraint is indeed enforced.
     *
     * @throws Exception if any issues occur.
     */
    public void testUniqueConstraint() throws Throwable {
        // Try adding a non unique value to a. Table is test_unique_ops
        CommandResult result;
        result = server.doCommand(
            "INSERT INTO test_unique_ops VALUES (1, 'purple', 60)", false);
        if (!result.failed()) {
            result = server.doCommand(
                "DELETE FROM test_unique_ops WHERE a=1 AND b='purple' " +
                "AND c=60", false);
            assert false;
        }
    }


	 /**
     * This test checks that dropping a unique index removes the unique
     * constraint.
     *
     * @throws Exception if any issues occur.
     */
    public void testDropUniqueIndex() throws Throwable {
        // Perform the result to create the index. Table is test_unique_ops
        CommandResult result;
        result = server.doCommand(
            "CREATE UNIQUE INDEX idx_test2 ON test_unique_ops (c)", false);
        // Check that unique constraint has been made
        result = server.doCommand(
            "INSERT INTO test_unique_ops VALUES (7, 'pink', 10)", false);
        if (!result.failed()) {
            result = server.doCommand(
                "DELETE FROM test_unique_ops WHERE a=7 AND b='pink' " +
                "AND c=10", false);
            assert false;
        }
        result = server.doCommand(
            "DROP INDEX idx_test2 ON test_unique_ops", false);
        StorageManager storageManager = server.getStorageManager();
        TableManager tableManager = storageManager.getTableManager();
        TableInfo tableInfo = tableManager.openTable(
            "test_unique_ops");
        // Check tblFileInfo has the index deleted from it
        TableSchema schema = tableInfo.getSchema();
        for (String indexName : schema.getIndexes().keySet()) {
            if (indexName.equals("idx_test2")) {
                assert false;
            }
        }

        // Check that unique constraint has been dropped
        result = server.doCommand(
            "INSERT INTO test_unique_ops VALUES (8, 'mauve', 10)", false);
        if (result.failed()) {
            assert false;
        }
        // Restore table
        result = server.doCommand(
            "DELETE FROM test_unique_ops WHERE a=8 AND b='mauve' " +
            "AND c=10", false);
    }


	 /**
     * This test checks to see if the CREATE UNIQUE INDEX command searches
     * the relevant columns to see if the populated values are unique. It
     * does this by inserting a nonunique value into c, and then attempts
     * to create a unique index on c afterwards, which should fail.
     *
     * @throws Exception if any issues occur.
     */
    public void testUniquePopulated() throws Throwable {
        // Perform the result to create the index. Table is test_unique_ops
        CommandResult result;
        result = server.doCommand(
            "INSERT INTO test_unique_ops VALUES (10, 'taupe', 10)", false);
        result = server.doCommand(
            "CREATE UNIQUE INDEX idx_test3 ON test_unique_ops (c)", false);
        // Index should not be able to be created because c is not unique
        if (!result.failed()) {
            result = server.doCommand(
                "DROP INDEX idx_test3 ON test_unique_ops", false);
            assert false;
        }
        result = server.doCommand(
            "DELETE FROM test_unique_ops WHERE a=10 AND b='taupe' " +
            "AND c=10", false);
    }
}
