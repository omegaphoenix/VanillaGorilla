package edu.caltech.test.nanodb.sql;


import edu.caltech.nanodb.expressions.TupleLiteral;
import edu.caltech.nanodb.relations.Schema;
import edu.caltech.nanodb.server.CommandResult;
import org.testng.annotations.Test;


/**
 * This class performs some basic tests with  inner joins, left-outer joins, and
 * right-outer joins.
 */
@Test
public class TestSimpleJoins extends SqlTestCase {
    public TestSimpleJoins() {
        super("setup_testSimpleJoins");
    }

    /**
     * This test checks that at least one value was successfully inserted into
     * each of the test tables.
     *
     * @throws Exception if any query parsing or execution issues occur.
     **/
    public void testSimpleTablesNotEmpty() throws Throwable {
        testTableNotEmpty("test_sj_t1");
        testTableNotEmpty("test_sj_t2");
        testTableNotEmpty("test_sj_t3");
        testTableNotEmpty("test_sj_t4");
    }

    /**
     * This test performs inner joins, and left/right outer joins with two tables, verifying both the
     * schema and the data that is returned.
     *
     * @throws Exception if any query parsing or execution issues occur.
     **/
    public void testSimpleJoinTwoTables() throws Throwable {
        CommandResult result;

        // Inner join with only one common column.
        result = server.doCommand(
            "SELECT * FROM test_sj_t1 t1 JOIN test_sj_t3 t3 ON t1.a = t3.a", true);
        TupleLiteral[] expected1 = {
            new TupleLiteral(2, 20, 2, 200, 2000),
            new TupleLiteral(3, 0, 3, 300, 3000),
            new TupleLiteral(3, 30, 3, 300, 3000),
            new TupleLiteral(5, 50, 5, 500, 5000),
            new TupleLiteral(6, 60, 6, 600, 6000),
            new TupleLiteral(8, 80, 8, 800, 8000)
        };
        assert checkSizeResults(expected1, result);
        assert checkUnorderedResults(expected1, result);
        checkResultSchema(result, "T1.A", "T1.B", "T3.A", "T3.C", "T3.D");

        // Left join
        result = server.doCommand(
            "SELECT * FROM test_sj_t1 t1 LEFT JOIN test_sj_t3 t3", true);
        TupleLiteral[] expected2 = {
            new TupleLiteral(1, 10, null, null),
            new TupleLiteral(2, 20, 200, 2000),
            new TupleLiteral(3, 0, 300, 3000),
            new TupleLiteral(3, 30, 300, 3000),
            new TupleLiteral(4, 40, null, null),
            new TupleLiteral(5, 50, 500, 5000),
            new TupleLiteral(6, 60, 600, 6000),
            new TupleLiteral(7, 70, null, null),
            new TupleLiteral(8, 80, 800, 8000)
        };
        assert checkSizeResults(expected2, result);
        assert checkUnorderedResults(expected2, result);
        checkResultSchema(result, "A", "T1.B", "T3.C", "T3.D");
    }

    /**
     * This test checks for joins where the left table is empty.
     *
     * @throws Exception if any query parsing or execution issues occur.
     **/
    public void testSimpleJoinEmptyLeft() throws Throwable {
        CommandResult result;
        // Inner Join
        result = server.doCommand(
                "SELECT * FROM test_empty1 e JOIN test_sj_t1 t1 ON e.a = t1.a", true);
        TupleLiteral[] expected1 = {};
        assert checkSizeResults(expected1, result);
        assert checkUnorderedResults(expected1, result);
        checkResultSchema(result, "E.A", "E.D", "T1.A", "T1.B");

        // Left Join should be empty.
        result = server.doCommand(
                "SELECT * FROM test_empty1 e LEFT JOIN test_sj_t1 t1;", true);
        assert checkSizeResults(expected1, result);
        assert checkUnorderedResults(expected1, result);
        checkResultSchema(result, "E.A", "E.D", "T1.A", "T1.B");

        // Right join contain the values in T1, with nulls with the empty table columns.
        result = server.doCommand(
                "SELECT * FROM test_empty1 e RIGHT JOIN test_sj_t1 t1;", true);
        TupleLiteral[] expected2 = {
            new TupleLiteral(null, null, 1, 10),
            new TupleLiteral(null, null, 2, 20),
            new TupleLiteral(null, null, 3, 30),
            new TupleLiteral(null, null, 3, 0),
            new TupleLiteral(null, null, 4, 10),
            new TupleLiteral(null, null, 5, 50),
            new TupleLiteral(null, null, 6, 60),
            new TupleLiteral(null, null, 7, 70),
            new TupleLiteral(null, null, 8, 80)
        };
        assert checkSizeResults(expected2, result);
        assert checkUnorderedResults(expected2, result);
        checkResultSchema(result, "E.A", "E.D", "T1.A", "T1.B");
    }


    /**
     * This test checks for joins where the right table is empty.
     *
     * @throws Exception if any query parsing or execution issues occur.
     **/
    public void testSimpleJoinEmptyRight() throws Throwable {
        CommandResult result;
        // Inner Join
        result = server.doCommand(
                "SELECT * FROM test_sj_t1 t1 JOIN test_empty1 e ON e.a = t1.a", true);
        TupleLiteral[] expected1 = {};
        assert checkSizeResults(expected1, result);
        assert checkUnorderedResults(expected1, result);
        checkResultSchema(result, "T1.A", "T1.B", "E.A", "E.D");

        // Right Join should be empty.
        result = server.doCommand(
                "SELECT * FROM test_sj_t1 t1 RIGHT JOIN test_empty1 e;", true);
        assert checkSizeResults(expected1, result);
        assert checkUnorderedResults(expected1, result);
        checkResultSchema(result, "T1.A", "T1.B", "E.A", "E.D");

        // Left join contain the values in T1, with nulls with the empty table columns.
        result = server.doCommand(
                "SELECT * FROM test_sj_t1 t1 LEFT JOIN test_empty1 e;", true);
        TupleLiteral[] expected2 = {
            new TupleLiteral(1, 10, null, null),
            new TupleLiteral(2, 20, null, null),
            new TupleLiteral(3, 30, null, null),
            new TupleLiteral(3, 0, null, null),
            new TupleLiteral(4, 40, null, null),
            new TupleLiteral(5, 50, null, null),
            new TupleLiteral(6, 60, null, null),
            new TupleLiteral(7, 70, null, null),
            new TupleLiteral(8, 80, null, null)
        };
        assert checkSizeResults(expected2, result);
        assert checkUnorderedResults(expected2, result);
        checkResultSchema(result, "T1.A", "T1.B", "E.A", "E.D");
    }

    /**
     * This test performs natural joins with three or more tables, verifying
     * both the schema and the data that is returned.  Joining this many
     * tables ensures that joins will operate against columns without table
     * names.
     *
     * @throws Exception if any query parsing or execution issues occur.
     **/
    public void testSimpleJoinMultiTables() throws Throwable {
        CommandResult result;

        // NATURAL JOIN three tables with a common column name A.
        // The first two tables are joined first, which means there will be
        // a column "A" without a table name, joined to the third table.
        result = server.doCommand(
            "SELECT * FROM (test_sj_t1 t1 JOIN test_sj_t4 t4 ON t1.a = t4.a) JOIN test_sj_t5 t5", true);
        TupleLiteral[] expected1 = {
            new TupleLiteral(2, 20, 200, 2000),
            new TupleLiteral(5, 50, 600, 5000),
            new TupleLiteral(6, 60, 500, 6000)
        };
        assert checkSizeResults(expected1, result);
        assert checkUnorderedResults(expected1, result);
        checkResultSchema(result, "A", "T1.B", "T4.C", "T5.D");
    }


    /**
     * This test performs joins with a <tt>USING</tt> clause, with three or
     * more tables, verifying both the schema and the data that is returned.
     * Joining this many tables ensures that joins will operate against
     * columns without table names.
     *
     * @throws Exception if any query parsing or execution issues occur.
     **/
    public void testUsingJoinMultiTables() throws Throwable {
        CommandResult result;

        result = server.doCommand(
            "SELECT * FROM (test_sj_t1 t1 JOIN test_sj_t4 t4 USING (A)) JOIN test_sj_t5 t5 USING (A)", true);
        TupleLiteral[] expected1 = {
            new TupleLiteral(2, 20, 200, 2000),
            new TupleLiteral(5, 50, 600, 5000),
            new TupleLiteral(6, 60, 500, 6000)
        };
        assert checkSizeResults(expected1, result);
        assert checkUnorderedResults(expected1, result);
        checkResultSchema(result, "A", "T1.B", "T4.C", "T5.D");


        result = server.doCommand(
            "SELECT * FROM test_sj_t1 t1 JOIN (test_sj_t4 t4 JOIN test_sj_t5 t5 USING (A)) USING (A)", true);
        assert checkSizeResults(expected1, result);
        assert checkUnorderedResults(expected1, result);
        checkResultSchema(result, "A", "T1.B", "T4.C", "T5.D");


        result = server.doCommand(
            "SELECT * FROM (test_sj_t1 t1 JOIN test_sj_t4 t4 USING (A)) " +
                "JOIN (test_sj_t5 t5 JOIN test_sj_t6 t6 USING (A)) USING (A)", true);
        TupleLiteral[] expected2 = {
            new TupleLiteral(2, 20, 200, 2000, 20000),
            new TupleLiteral(5, 50, 600, 5000, 60000),
            new TupleLiteral(6, 60, 500, 6000, 50000)
        };
        assert checkSizeResults(expected2, result);
        assert checkUnorderedResults(expected2, result);
        checkResultSchema(result, "A", "T1.B", "T4.C", "T5.D", "T6.E");
    }
}
