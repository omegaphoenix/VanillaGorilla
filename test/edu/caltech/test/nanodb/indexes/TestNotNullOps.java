package edu.caltech.test.nanodb.indexes;


import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import edu.caltech.test.nanodb.sql.SqlTestCase;
import edu.caltech.nanodb.server.CommandResult;
import edu.caltech.nanodb.server.NanoDBServer;

/**
 * This class exercises the database with some simple INSERT statements
 * on a table with NOT NULL constraints, to see if the NOT NULL constraint
 * works propertly.
 */

@Test
public class TestNotNullOps extends SqlTestCase {

    public TestNotNullOps() {
        super("setup_testNotNullOps");
    }

    @BeforeClass
    public void setup() {
        /*
        StorageManager storageManager;
        TableInfo tblFileInfo;
        try {
            storageManager = StorageManager.getInstance();
            tblFileInfo = storageManager.openTable(
                "test_not_null_ops");
        } catch (Exception e) {
        }
        */
    }

   
   	/**
     * This test checks that the not null constraint added to column a 
     * and column b is enforced. It does this by trying to insert a 
     * variety of tuples with null values in either column a or
     * column b, and that these insert commands should fail if the not null
     * constraint is indeed enforced.
     *
     * @throws Exception if any issues occur.
     */
    public void testNotNullConstraint() throws Throwable {
        // Try adding a null value to a. Table is test_not_null_ops
        CommandResult result;

        // Should fail since a has a NOT NULL constraint
        result = server.doCommand(
            "INSERT INTO test_not_null_ops VALUES (null, 'yellow', 30)", false);
        if (!result.failed()) {
            result = server.doCommand(
                "DELETE FROM test_not_null_ops WHERE a=null AND b='yellow' " +
                "AND c=30", false);
            assert false;
        }

        // Try adding a null value to b, which has a  NOT NULL and a 
        // UNIQUE constraint on it (since it is a PRIMARY KEY) 
        // to see if the two constraints somehow conflict.
        result = server.doCommand(
            "INSERT INTO test_not_null_ops VALUES (1, null, 40)", false);
        if (!result.failed()) {
            result = server.doCommand(
                "DELETE FROM test_not_null_ops WHERE a=1 AND b=null " +
                "AND c=40", false);
            assert false;
        }

        // Try adding a nonunique value to b, which has a  NOT NULL and a 
        // UNIQUE constraint on it (since it is a PRIMARY KEY) 
        // to see if the two constraints somehow conflict.
        result = server.doCommand(
            "INSERT INTO test_not_null_ops VALUES (1, 'red', 40)", false);
        if (!result.failed()) {
            result = server.doCommand(
                "DELETE FROM test_not_null_ops WHERE a=1 AND b='red' " +
                "AND c=40", false);
            assert false;
        }

        // Lastly, make sure adding null is possible to columns that don't
        // have the NOT NULL constraint. Particularly, we should be able
        // to insert a null into c.
        result = server.doCommand(
            "INSERT INTO test_not_null_ops VALUES (1, 'green', NULL)", false);
        if (result.failed()) {
            result.getFailure().printStackTrace();
            result = server.doCommand(
                "DELETE FROM test_not_null_ops WHERE a=1 AND b='green' " +
                "AND c IS NULL", false);
            assert false;
        }
    }
	
}
