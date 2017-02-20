package edu.caltech.test.nanodb.indexes;


import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import edu.caltech.test.nanodb.sql.SqlTestCase;
import edu.caltech.nanodb.expressions.TupleLiteral;
import edu.caltech.nanodb.server.CommandResult;
import edu.caltech.nanodb.server.NanoDBServer;

/**
 * This class exercises the database with some simple statements
 * on tables with FOREIGN KEY constraints, to see if the FOREIGN KEY
 * constraint works propertly.
 */

@Test
public class TestForeignKeyOps extends SqlTestCase {

    public TestForeignKeyOps() {
        super("setup_testForeignOps");
    }

    @BeforeClass
    public void setup() {
        /*
        StorageManager storageManager;
        TableInfo tblFileInfo;
        try {
            storageManager = StorageManager.getInstance();
            tblFileInfo = storageManager.openTable(
                "test_fkey_parent_ops");
        }
        catch (Exception e) {
        }
        */
    }

    /**
     * This test checks the ON DELETE RESTRICT and ON UPDATE RESTRICT 
     * foreign key options. It does so with child1_ops, which has no
     * options specified (so they default to restict) and tries to 
     * update the parent table. 
     *
     * @throws Exception if any issues occur.
     */
    public void testRestrictOption() throws Throwable {
        CommandResult result;
        tryDoCommand("CREATE TABLE test_fkey_parent1_ops (a INTEGER PRIMARY KEY)", false);
        tryDoCommand("INSERT INTO test_fkey_parent1_ops VALUES (1)", false);
        tryDoCommand("INSERT INTO test_fkey_parent1_ops VALUES (2)", false);
        /* Create a child table with RESTRICT options as default */
        tryDoCommand("CREATE TABLE test_fkey_child1_ops (id1 INTEGER, " +
                     "FOREIGN KEY (id1) REFERENCES test_fkey_parent1_ops(a))", false);
        tryDoCommand("INSERT INTO test_fkey_child1_ops VALUES (1)", false);

        /* Try deleting/updating from the parent table, this should fail
         * due to RESTRICT options */
        result = server.doCommand(
                "DELETE FROM test_fkey_parent1_ops WHERE a=1", false);
        if (!result.failed())
            assert false;

        result = server.doCommand(
                "UPDATE test_fkey_parent1_ops SET a=5 WHERE a=1", false);
        if (!result.failed())
            assert false;
    }

    /**
     * This test checks the ON DELETE CASCADE and ON UPDATE CASCADE 
     * foreign key options. It does so with child2_ops and tries to 
     * update the parent table. This test also checks that we can
     * use foreign keys with multiple columns.
     *
     * @throws Exception if any issues occur.
     */
    public void testCascadeOption() throws Throwable {
        CommandResult result;
        tryDoCommand("CREATE TABLE test_fkey_parent2_ops (a INTEGER, b INTEGER, " +
                     "UNIQUE (a,b))", false);
        tryDoCommand("INSERT INTO test_fkey_parent2_ops VALUES (1, 10)", false);
        tryDoCommand("INSERT INTO test_fkey_parent2_ops VALUES (2, 20)", false);
        /* Create a child table with CASCADE options */
        tryDoCommand("CREATE TABLE test_fkey_child2_ops (id1 INTEGER, id2 INTEGER, " +
                     "FOREIGN KEY (id1, id2) REFERENCES test_fkey_parent2_ops(a, b) " +
                     "ON DELETE CASCADE ON UPDATE CASCADE)", false);
        tryDoCommand("INSERT INTO test_fkey_child2_ops VALUES (1, 10)", false);
        tryDoCommand("INSERT INTO test_fkey_child2_ops VALUES (1, 10)", false);
        tryDoCommand("INSERT INTO test_fkey_child2_ops VALUES (2, 20)", false);

        /* Try deleting/updating from the parent table */
        tryDoCommand("UPDATE test_fkey_parent2_ops SET a = 4 WHERE a = 1", false);
        result = server.doCommand("SELECT * FROM test_fkey_child2_ops", true);
        TupleLiteral[] expected1 = {
            new TupleLiteral(4, 10),
            new TupleLiteral(4, 10),
            new TupleLiteral(2, 20)
        };
        assert checkUnorderedResults(expected1, result);

        tryDoCommand("DELETE FROM test_fkey_parent2_ops WHERE a=4", false);
        result = tryDoCommand("SELECT * FROM test_fkey_parent2_ops", true);
        TupleLiteral[] expected2 = {
            new TupleLiteral(2, 20)
        };
        assert checkUnorderedResults(expected2, result);
    }


    /**
     * This test checks the ON DELETE SET NULL and ON UPDATE SET NULL 
     * foreign key options. It does so with child3_ops and tries to 
     * update the parent table. 
     *
     * @throws Exception if any issues occur.
     */
    public void testSetNullOption() throws Throwable {
        CommandResult result;

        tryDoCommand("CREATE TABLE test_fkey_parent3_ops (a INTEGER PRIMARY KEY)", false);
        tryDoCommand("INSERT INTO test_fkey_parent3_ops VALUES (1)", false);
        tryDoCommand("INSERT INTO test_fkey_parent3_ops VALUES (2)", false);

        /* Create a child table with SET NULL options */
        tryDoCommand("CREATE TABLE test_fkey_child3_ops (id1 INTEGER, id2 INTEGER, " +
                     "FOREIGN KEY (id2) REFERENCES test_fkey_parent3_ops(a) " +
                     "ON DELETE SET NULL ON UPDATE SET NULL)", false);
        tryDoCommand("INSERT INTO test_fkey_child3_ops VALUES (10, 1)", false);
        tryDoCommand("INSERT INTO test_fkey_child3_ops VALUES (100, 1)", false);
        tryDoCommand("INSERT INTO test_fkey_child3_ops VALUES (20, 2)", false);

        /* Try deleting/updating from the parent table */
        tryDoCommand("UPDATE test_fkey_parent3_ops SET a = 4 WHERE a = 1", false);
        result = tryDoCommand("SELECT * FROM test_fkey_child3_ops", true);
        TupleLiteral[] expected1 = {
            new TupleLiteral(10, null),
            new TupleLiteral(100, null),
            new TupleLiteral(20, 2)
        };
        assert checkUnorderedResults(expected1, result);

        tryDoCommand("DELETE FROM test_fkey_parent3_ops WHERE a=4", false);
        result = tryDoCommand("SELECT * FROM test_fkey_child3_ops", true);
        TupleLiteral[] expected2 = {
            new TupleLiteral(10, null),
            new TupleLiteral(100, null),
            new TupleLiteral(20, 2)
        };
        assert checkUnorderedResults(expected2, result);
    }

    /**
     * This test checks that we aren't able to make foreign keys that use
     * a different number of columns between two tables.
     *
     * @throws Exception if any issues occur.
     */
    public void testFKeySize() throws Throwable {
        CommandResult result;
        tryDoCommand("CREATE TABLE test_fkey_parent4_ops" +
                     " (a INTEGER, b INTEGER, UNIQUE (a, b))", false);

        // Create a child table that tries to create a foreign key, but the
        // column numbers don't match.
        result = server.doCommand(
            "CREATE TABLE test_fkey_child4_ops (id1 INTEGER, id2 INTEGER, " +
            "FOREIGN KEY (id1) REFERENCES test_fkey_parent4_ops(a, b))", false);

        if (!result.failed())
            assert false;
    }

    /**
     * This test checks that we aren't able to make foreign keys between two
     * columns that aren't the same type.
     *
     * @throws Exception if any issues occur.
     */
    public void testFKeyType() throws Throwable {
        CommandResult result;
        result = tryDoCommand(
            "CREATE TABLE test_fkey_parent5_ops (a VARCHAR(20), " +
            "b VARCHAR(20), UNIQUE (a, b))", false);
        /* Create a child table that tries to create a foreign key, but the
         * column numbers don't match */
        result = server.doCommand(
                "CREATE TABLE test_fkey_child5_ops (id1 INTEGER, id2 INTEGER, " +
                "FOREIGN KEY (id1) REFERENCES test_fkey_parent5_ops(a, b))", false);

        if (!result.failed())
            assert false;
    }
}
