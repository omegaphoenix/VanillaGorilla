package edu.caltech.test.nanodb.storage.btreefile;


import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.Random;

import edu.caltech.nanodb.expressions.ColumnName;
import edu.caltech.nanodb.expressions.ColumnValue;
import edu.caltech.nanodb.expressions.OrderByExpression;
import edu.caltech.nanodb.expressions.TupleComparator;
import edu.caltech.nanodb.expressions.TupleLiteral;
import edu.caltech.nanodb.relations.ColumnInfo;
import edu.caltech.nanodb.relations.ColumnType;
import edu.caltech.nanodb.relations.SQLDataType;
import edu.caltech.nanodb.relations.Schema;
import edu.caltech.nanodb.server.CommandResult;
import org.testng.annotations.*;

import edu.caltech.test.nanodb.sql.SqlTestCase;


/**
 * This test class exercises the B<sup>+</sup>-tree file format so that we can
 * have some confidence that it actually works correctly.
 */
@Test
public class TestBTreeFile extends SqlTestCase {

    /**
     * If set to true, this causes the tests to verify the contents of the
     * B tree file after every insertion or deletion.  This obviously greatly
     * slows down the test, but it allows issues to be identified exactly when
     * they appear.
     */
    public static final boolean CHECK_AFTER_EACH_CHANGE = false;


    /**
     * A source of randomness to generate tuples from.  Set the seed so we
     * have reproducible test cases.
     */
    private Random rand = new Random(12345);


    private String makeRandomString(int minChars, int maxChars) {
        StringBuilder buf = new StringBuilder();

        int num = minChars + rand.nextInt(maxChars - minChars + 1);
        for (int i = 0; i < num; i++)
            buf.append((char) ('A' + rand.nextInt('Z' - 'A' + 1)));

        return buf.toString();
    }


    private void sortTupleLiteralArray(ArrayList<TupleLiteral> tuples) {
        Schema schema = new Schema();
        schema.addColumnInfo(new ColumnInfo("a", new ColumnType(SQLDataType.INTEGER)));
        schema.addColumnInfo(new ColumnInfo("b", new ColumnType(SQLDataType.VARCHAR)));

        ArrayList<OrderByExpression> orderSpec = new ArrayList<>();
        orderSpec.add(new OrderByExpression(new ColumnValue(new ColumnName("a"))));
        orderSpec.add(new OrderByExpression(new ColumnValue(new ColumnName("b"))));

        TupleComparator comp = new TupleComparator(schema, orderSpec);
        Collections.sort(tuples, comp);
    }


    private void runBTreeTest(String tableName, int numRowsToInsert,
                              int maxAValue, int minBLen, int maxBLen,
                              double probDeletion) throws Exception {
        ArrayList<TupleLiteral> inserted = new ArrayList<>();
        CommandResult result;

        for (int i = 0; i < numRowsToInsert; i++) {
            int a = rand.nextInt(maxAValue);
            String b = makeRandomString(minBLen, maxBLen);

            tryDoCommand(String.format(
                "INSERT INTO %s VALUES (%d, '%s');", tableName, a, b), false);
            inserted.add(new TupleLiteral(a, b));

            if (CHECK_AFTER_EACH_CHANGE) {
                sortTupleLiteralArray(inserted);
                result = tryDoCommand(String.format("SELECT * FROM %s;",
                    tableName), true);
                assert checkOrderedResults(inserted.toArray(new TupleLiteral[inserted.size()]), result);
            }

            if (probDeletion > 0.0 && rand.nextDouble() < probDeletion) {
                // Delete some rows from the table we are populating.
                int minAToDel = rand.nextInt(maxAValue);
                int maxAToDel = rand.nextInt(maxAValue);
                if (minAToDel > maxAToDel) {
                    int tmp = minAToDel;
                    minAToDel = maxAToDel;
                    maxAToDel = tmp;
                }

                tryDoCommand(String.format(
                    "DELETE FROM %s WHERE a BETWEEN %d AND %d;", tableName,
                    minAToDel, maxAToDel), false);

                // Apply the same deletion to our in-memory collection of
                // tuples, so that we can mirror what the table should contain
                Iterator<TupleLiteral> iter = inserted.iterator();
                while (iter.hasNext()) {
                    TupleLiteral tup = iter.next();
                    int aVal = (Integer) tup.getColumnValue(0);
                    if (aVal >= minAToDel && aVal <= maxAToDel)
                        iter.remove();
                }

                if (CHECK_AFTER_EACH_CHANGE) {
                    sortTupleLiteralArray(inserted);
                    result = tryDoCommand(String.format("SELECT * FROM %s;",
                        tableName), true);
                    assert checkOrderedResults(inserted.toArray(new TupleLiteral[inserted.size()]), result);
                }
            }
        }

        sortTupleLiteralArray(inserted);
        result = tryDoCommand(String.format("SELECT * FROM %s;",
            tableName), true);
        assert checkOrderedResults(inserted.toArray(new TupleLiteral[inserted.size()]), result);

        // TODO:  This is necessary because the btree code doesn't unpin
        //        pages properly...
        // server.getStorageManager().flushAllData();
    }


    public void testBTreeTableOnePageInsert() throws Exception {
        tryDoCommand("CREATE TABLE btree_one_page (a INTEGER, b VARCHAR(20)) " +
            "PROPERTIES (storage = 'btree');", false);

        runBTreeTest("btree_one_page", 300, 200, 3, 20, 0.0);
    }


    public void testBTreeTableTwoPageInsert() throws Exception {
        tryDoCommand("CREATE TABLE btree_two_page (a INTEGER, b VARCHAR(50)) " +
            "PROPERTIES (storage = 'btree');", false);

        runBTreeTest("btree_two_page", 400, 200, 20, 50, 0.0);
    }


    public void testBTreeTableTwoLevelInsert() throws Exception {
        tryDoCommand("CREATE TABLE btree_two_level (a INTEGER, b VARCHAR(50)) " +
            "PROPERTIES (storage = 'btree');", false);

        runBTreeTest("btree_two_level", 10000, 1000, 20, 50, 0.0);
    }


    public void testBTreeTableThreeLevelInsert() throws Exception {
        tryDoCommand("CREATE TABLE btree_three_level (a INTEGER, b VARCHAR(250)) " +
            "PROPERTIES (storage = 'btree');", false);

        runBTreeTest("btree_three_level", 100000, 5000, 150, 250, 0.0);
    }


    public void testBTreeTableOnePageInsertDelete() throws Exception {
        tryDoCommand("CREATE TABLE btree_one_page_del (a INTEGER, b VARCHAR(20)) " +
            "PROPERTIES (storage = 'btree');", false);

        runBTreeTest("btree_one_page_del", 400, 200, 3, 20, 0.05);
    }


    public void testBTreeTableTwoPageInsertDelete() throws Exception {
        tryDoCommand("CREATE TABLE btree_two_page_del (a INTEGER, b VARCHAR(50)) " +
            "PROPERTIES (storage = 'btree');", false);

        runBTreeTest("btree_two_page_del", 500, 200, 20, 50, 0.05);
    }


    public void testBTreeTableTwoLevelInsertDelete() throws Exception {
        tryDoCommand("CREATE TABLE btree_two_level_del (a INTEGER, b VARCHAR(50)) " +
            "PROPERTIES (storage = 'btree');", false);

        runBTreeTest("btree_two_level_del", 12000, 1000, 20, 50, 0.05);
    }


    public void testBTreeTableThreeLevelInsertDelete() throws Exception {
        tryDoCommand("CREATE TABLE btree_three_level_del (a INTEGER, b VARCHAR(250)) " +
            "PROPERTIES (storage = 'btree');", false);

        runBTreeTest("btree_three_level_del", 120000, 5000, 150, 250, 0.05);
    }


    public void testBTreeTableMultiLevelInsertDelete() throws Exception {
        tryDoCommand("CREATE TABLE btree_multi_level_del (a INTEGER, b VARCHAR(400)) " +
            "PROPERTIES (storage = 'btree');", false);

        runBTreeTest("btree_multi_level_del", 250000, 5000, 50, 400, 0.01);
    }
}
