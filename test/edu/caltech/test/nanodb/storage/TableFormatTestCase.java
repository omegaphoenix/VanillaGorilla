package edu.caltech.test.nanodb.storage;


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
import edu.caltech.test.nanodb.sql.SqlTestCase;


/**
 * This is a base-class for tests that exercise tables of a specific format.
 * It provides general options for generating tuples, sorting tuples, and
 * so forth.
 */
public class TableFormatTestCase extends SqlTestCase {

    /**
     * A source of randomness to generate tuples from.  Set the seed so we
     * have reproducible test cases.
     */
    protected Random rand = new Random(12345);


    protected String makeRandomString(int minChars, int maxChars) {
        StringBuilder buf = new StringBuilder();

        int num = minChars + rand.nextInt(maxChars - minChars + 1);
        for (int i = 0; i < num; i++)
            buf.append((char) ('A' + rand.nextInt('Z' - 'A' + 1)));

        return buf.toString();
    }


    protected void sortTupleLiteralArray(ArrayList<TupleLiteral> tuples) {
        Schema schema = new Schema();
        schema.addColumnInfo(new ColumnInfo("a", new ColumnType(SQLDataType.INTEGER)));
        schema.addColumnInfo(new ColumnInfo("b", new ColumnType(SQLDataType.VARCHAR)));

        ArrayList<OrderByExpression> orderSpec = new ArrayList<>();
        orderSpec.add(new OrderByExpression(new ColumnValue(new ColumnName("a"))));
        orderSpec.add(new OrderByExpression(new ColumnValue(new ColumnName("b"))));

        TupleComparator comp = new TupleComparator(schema, orderSpec);
        Collections.sort(tuples, comp);
    }


    /**
     * This helper function generates random rows to insert into a table,
     * and then verifies that the rows
     * @param tableName The name of the table to insert rows into.
     * @param numRowsToInsert The total number of rows to insert.
     * @param maxAValue maximum integer value to generate for INTEGER column a.
     *        This can be used to generate records that will collide, for
     *        sequential and heap storage formats
     * @param minBLen minimum string length for VARCHAR column b.
     * @param maxBLen maximum string length for VARCHAR column b.
     * @param ordered true if the storage format should produce the tuples in
     *        order (a, b), false if the tuples will be unordered
     * @param deleteWhileInserting if true, some tuples will periodically be
     *        deleted.  The function records which tuples should be deleted
     *        so that the actual contents of the table file can be compared
     *        to the expected contents of the table file.
     * @throws Exception if an IO error occurs, or if the test fails.
     */
    protected void insertRows(String tableName, int numRowsToInsert,
        int maxAValue, int minBLen, int maxBLen, boolean ordered,
        boolean deleteWhileInserting) throws Exception {

        ArrayList<TupleLiteral> inserted = new ArrayList<TupleLiteral>();
        for (int i = 0; i < numRowsToInsert; i++) {
            int a = rand.nextInt(maxAValue);
            String b = makeRandomString(minBLen, maxBLen);

            tryDoCommand(String.format(
                "INSERT INTO %s VALUES (%d, '%s');", tableName, a, b), false);
            inserted.add(new TupleLiteral(a, b));

            if (deleteWhileInserting && rand.nextDouble() < 0.05) {
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
            }
        }

        if (ordered)
            sortTupleLiteralArray(inserted);

        CommandResult result = tryDoCommand(String.format("SELECT * FROM %s;",
            tableName), true);

        TupleLiteral[] tupleArray = inserted.toArray(new TupleLiteral[inserted.size()]);
        if (ordered)
            assert checkOrderedResults(tupleArray, result);
        else
            assert checkUnorderedResults(tupleArray, result);
    }
}
