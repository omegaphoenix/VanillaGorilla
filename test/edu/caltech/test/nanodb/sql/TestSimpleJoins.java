package edu.caltech.test.nanodb.sql;


import edu.caltech.nanodb.expressions.TupleLiteral;
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
        try {
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
        }
        catch (AssertionError ae) {
            TupleLiteral[] expected1 = {
                    new TupleLiteral(2, 200, 2000, 2, 20),
                    new TupleLiteral(3, 300, 3000, 3, 0),
                    new TupleLiteral(3, 300, 3000, 3, 30),
                    new TupleLiteral(5, 500, 5000, 5, 50),
                    new TupleLiteral(6, 600, 6000, 6, 60),
                    new TupleLiteral(8, 800, 8000, 8, 80)
            };
            assert checkSizeResults(expected1, result);
            assert checkUnorderedResults(expected1, result);
            checkResultSchema(result, "T3.A", "T3.C", "T3.D", "T1.A", "T1.B");
        }

        // Left join
        result = server.doCommand(
            "SELECT * FROM test_sj_t1 t1 LEFT JOIN test_sj_t3 t3 ON t1.a = t3.a", true);
        TupleLiteral[] expected2 = {
            new TupleLiteral(1, 10, null, null, null),
            new TupleLiteral(2, 20, 2, 200, 2000),
            new TupleLiteral(3, 0, 3, 300, 3000),
            new TupleLiteral(3, 30, 3, 300, 3000),
            new TupleLiteral(4, 40, null, null, null),
            new TupleLiteral(5, 50, 5, 500, 5000),
            new TupleLiteral(6, 60, 6, 600, 6000),
            new TupleLiteral(7, 70, null, null, null),
            new TupleLiteral(8, 80, 8, 800, 8000)
        };
        assert checkSizeResults(expected2, result);
        assert checkUnorderedResults(expected2, result);
        checkResultSchema(result, "T1.A", "T1.B", "T3.A", "T3.C", "T3.D");

        // Right join
        result = server.doCommand(
                "SELECT * FROM test_sj_t1 t1 RIGHT JOIN test_sj_t3 t3 ON t1.a = t3.a", true);
        TupleLiteral[] expected3 = {
            new TupleLiteral(null, null, 0, 0, 0),
            new TupleLiteral(2, 20, 2, 200, 2000),
            new TupleLiteral(3, 0, 3, 300, 3000),
            new TupleLiteral(3, 30, 3, 300, 3000),
            new TupleLiteral(5, 50, 5, 500, 5000),
            new TupleLiteral(6, 60, 6, 600, 6000),
            new TupleLiteral(8, 80, 8, 800, 8000),
            new TupleLiteral(null, null, 9, 900, 9000),
            new TupleLiteral(null, null, 11, 1100, 11000)
        };
        assert checkSizeResults(expected3, result);
        assert checkUnorderedResults(expected3, result);
        checkResultSchema(result, "T1.A", "T1.B", "T3.A", "T3.C", "T3.D");
    }

    /**
     * This test performs a cross join with two tables, verifying both the
     * schema and the data that is returned.
     *
     * @throws Exception if any query parsing or execution issues occur.
     **/
    public void testSimpleCrossJoinTwoTables() throws Throwable {
        CommandResult result;

        // Inner join with only one common column.
        try {
            result = server.doCommand(
                    "SELECT * FROM test_sj_t4 t4 CROSS JOIN test_sj_t5 t5", true);
            TupleLiteral[] expected1 = {
                new TupleLiteral(2, 200, 1, 1000),
                new TupleLiteral(2, 200, 2, 2000),
                new TupleLiteral(2, 200, 3, 3000),
                new TupleLiteral(2, 200, 4, 4000),
                new TupleLiteral(2, 200, 5, 5000),
                new TupleLiteral(2, 200, 6, 6000),

                new TupleLiteral(5, 600, 1, 1000),
                new TupleLiteral(5, 600, 2, 2000),
                new TupleLiteral(5, 600, 3, 3000),
                new TupleLiteral(5, 600, 4, 4000),
                new TupleLiteral(5, 600, 5, 5000),
                new TupleLiteral(5, 600, 6, 6000),

                new TupleLiteral(6, 500, 1, 1000),
                new TupleLiteral(6, 500, 2, 2000),
                new TupleLiteral(6, 500, 3, 3000),
                new TupleLiteral(6, 500, 4, 4000),
                new TupleLiteral(6, 500, 5, 5000),
                new TupleLiteral(6, 500, 6, 6000),

                new TupleLiteral(9, 1100, 1, 1000),
                new TupleLiteral(9, 1100, 2, 2000),
                new TupleLiteral(9, 1100, 3, 3000),
                new TupleLiteral(9, 1100, 4, 4000),
                new TupleLiteral(9, 1100, 5, 5000),
                new TupleLiteral(9, 1100, 6, 6000),

                new TupleLiteral(14, 1000, 1, 1000),
                new TupleLiteral(14, 1000, 2, 2000),
                new TupleLiteral(14, 1000, 3, 3000),
                new TupleLiteral(14, 1000, 4, 4000),
                new TupleLiteral(14, 1000, 5, 5000),
                new TupleLiteral(14, 1000, 6, 6000)
            };
            assert checkSizeResults(expected1, result);
            assert checkUnorderedResults(expected1, result);
            checkResultSchema(result, "T4.A", "T4.C", "T5.A", "T5.D");
        }
        catch (AssertionError ea) {
            result = server.doCommand(
                    "SELECT * FROM test_sj_t4 t4 CROSS JOIN test_sj_t5 t5", true);
            TupleLiteral[] expected1 = {
                    new TupleLiteral(1, 1000, 2, 200),
                    new TupleLiteral(2, 2000, 2, 200),
                    new TupleLiteral(2, 3000, 3, 200),
                    new TupleLiteral(2, 4000, 4, 200),
                    new TupleLiteral(2, 5000, 5, 200),
                    new TupleLiteral(2, 6000, 6, 200),

                    new TupleLiteral(5, 1000, 1, 600),
                    new TupleLiteral(5, 2000, 2, 600),
                    new TupleLiteral(5, 3000, 3, 600),
                    new TupleLiteral(5, 4000, 4, 600),
                    new TupleLiteral(5, 5000, 5, 600),
                    new TupleLiteral(5, 6000, 6, 600),

                    new TupleLiteral(6, 1000, 1, 500),
                    new TupleLiteral(6, 2000, 2, 500),
                    new TupleLiteral(6, 3000, 3, 500),
                    new TupleLiteral(6, 4000, 4, 500),
                    new TupleLiteral(6, 5000, 5, 500),
                    new TupleLiteral(6, 6000, 6, 500),

                    new TupleLiteral(9, 1000, 1, 1100),
                    new TupleLiteral(9, 2000, 2, 1100),
                    new TupleLiteral(9, 3000, 3, 1100),
                    new TupleLiteral(9, 4000, 4, 1100),
                    new TupleLiteral(9, 5000, 5, 1100),
                    new TupleLiteral(9, 6000, 6, 1100),

                    new TupleLiteral(14, 1000, 1, 1000),
                    new TupleLiteral(14, 2000, 2, 1000),
                    new TupleLiteral(14, 3000, 3, 1000),
                    new TupleLiteral(14, 4000, 4, 1000),
                    new TupleLiteral(14, 5000, 5, 1000),
                    new TupleLiteral(14, 6000, 6, 1000)
            };
            assert checkSizeResults(expected1, result);
            assert checkUnorderedResults(expected1, result);
            checkResultSchema(result, "T5.A", "T5.D", "T4.A", "T4.C");
        }
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

        try {
            checkResultSchema(result, "E.A", "E.D", "T1.A", "T1.B");
        }
        catch (AssertionError ae) {
            checkResultSchema(result, "T1.A", "T1.B", "E.A", "E.D");
        }

        // Left Join should be empty.
        result = server.doCommand(
                "SELECT * FROM test_empty1 e LEFT JOIN test_sj_t1 t1 ON e.a = t1.a", true);
        assert checkSizeResults(expected1, result);
        assert checkUnorderedResults(expected1, result);
        checkResultSchema(result, "E.A", "E.D", "T1.A", "T1.B");

        // Right join contain the values in T1, with nulls with the empty table columns.
        result = server.doCommand(
                "SELECT * FROM test_empty1 e RIGHT JOIN test_sj_t1 t1 ON e.a = t1.a", true);
        TupleLiteral[] expected2 = {
            new TupleLiteral(null, null, 1, 10),
            new TupleLiteral(null, null, 2, 20),
            new TupleLiteral(null, null, 3, 30),
            new TupleLiteral(null, null, 3, 0),
            new TupleLiteral(null, null, 4, 40),
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
        try {
            checkResultSchema(result, "T1.A", "T1.B", "E.A", "E.D");
        }
        catch (AssertionError ae) {
            checkResultSchema(result, "E.A", "E.D", "T1.A", "T1.B");
        }

        // Right Join should be empty.
        result = server.doCommand(
                "SELECT * FROM test_sj_t1 t1 RIGHT JOIN test_empty1 e ON t1.a = e.a", true);
        assert checkSizeResults(expected1, result);
        assert checkUnorderedResults(expected1, result);
        checkResultSchema(result, "T1.A", "T1.B", "E.A", "E.D");

        // Left join contain the values in T1, with nulls with the empty table columns.
        result = server.doCommand(
                "SELECT * FROM test_sj_t1 t1 LEFT JOIN test_empty1 e ON t1.a = e.a", true);
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
     * This test checks for joins where both tables are empty.
     *
     * @throws Exception if any query parsing or execution issues occur.
     **/
    public void testSimpleJoinBothEmpty() throws Throwable {
        CommandResult result;
        // Inner Join
        result = server.doCommand(
                "SELECT * FROM test_empty1 t1 JOIN test_empty2 t2 ON t1.a = t2.a", true);
        TupleLiteral[] expected1 = {};
        assert checkSizeResults(expected1, result);
        assert checkUnorderedResults(expected1, result);
        try {
            checkResultSchema(result, "T1.A", "T1.D", "T2.A", "T2.E");
        }
        catch (AssertionError ae) {
            checkResultSchema(result, "T2.A", "T2.E", "T1.A", "T1.D");
        }

        // Right Join should be empty.
        result = server.doCommand(
                "SELECT * FROM test_empty1 t1 RIGHT JOIN test_empty2 t2 on t1.a = t2.a", true);
        assert checkSizeResults(expected1, result);
        assert checkUnorderedResults(expected1, result);
        checkResultSchema(result, "T1.A", "T1.D", "T2.A", "T2.E");

        // Left join contain the values in T1, with nulls with the empty table columns.
        result = server.doCommand(
                "SELECT * FROM test_empty1 t1 LEFT JOIN test_empty2 t2 on t1.a = t2.a", true);
        assert checkSizeResults(expected1, result);
        assert checkUnorderedResults(expected1, result);
        checkResultSchema(result, "T1.A", "T1.D", "T2.A", "T2.E");
    }

    /**
     * This test performs a combination of inner and outer joins.
     *
     * @throws Exception if any query parsing or execution issues occur.
     **/
    public void testSimpleJoinMultiTables() throws Throwable {
        CommandResult result;

        // Join T1 and T4 first, then left join with an empty table.
        result = server.doCommand(
            "SELECT * FROM (test_sj_t1 t1 JOIN test_sj_t4 t4 ON t1.a = t4.a) LEFT JOIN test_empty1 e ON t1.a = e.a",
            true);
        try {
            TupleLiteral[] expected1 = {
                    new TupleLiteral(2, 20, 2, 200, null, null),
                    new TupleLiteral(5, 50, 5, 600, null, null),
                    new TupleLiteral(6, 60, 6, 500, null, null)
            };
            assert checkSizeResults(expected1, result);
            assert checkUnorderedResults(expected1, result);
            checkResultSchema(result, "T1.A", "T1.B", "T4.A", "T4.C", "E.A", "E.D");
        }
        catch (AssertionError ae) {
            TupleLiteral[] expected1 = {
                    new TupleLiteral(2, 200, 2, 20, null, null),
                    new TupleLiteral(5, 600, 5, 50, null, null),
                    new TupleLiteral(6, 500, 6, 60, null, null)
            };
            assert checkSizeResults(expected1, result);
            assert checkUnorderedResults(expected1, result);
            checkResultSchema(result, "T4.A", "T4.C", "T1.A", "T1.B", "E.A", "E.D");
        }
    }

    /**
     * This test goes through some basic edge cases for outer joins including tables with multiple rows
     * that match the same join predicate, and multi-column join predicates.
     *
     * @throws Exception if any query parsing or execution issues occur.
     **/
    public void testSimpleJoinEdgeCases() throws Throwable {
        CommandResult result;

        // For A = 3, 2 * 2 = 4 total rows should be generated in the left join, since there are two rows in T1
        // and two rows in T2 with A = 3.
        result = server.doCommand(
                "SELECT * FROM test_sj_t1 t1 LEFT JOIN test_sj_t2 t2 ON t1.a = t2.a",
                true);
        TupleLiteral[] expected1 = {
                new TupleLiteral(1, 10, null, null, null),
                new TupleLiteral(2, 20, null, null, null),
                new TupleLiteral(3, 30, 3, -1, -1),
                new TupleLiteral(3, 30, 3, 40, 300),
                new TupleLiteral(3, 0, 3, -1, -1),
                new TupleLiteral(3, 0, 3, 40, 300),
                new TupleLiteral(4, 40, 4, 30, 400),
                new TupleLiteral(5, 50, 5, 60, 500),
                new TupleLiteral(6, 60, 6, 50, 600),
                new TupleLiteral(7, 70, 7, 70, 700),
                new TupleLiteral(8, 80, 8, 80, 800)
        };
        assert checkSizeResults(expected1, result);
        assert checkUnorderedResults(expected1, result);
        checkResultSchema(result, "T1.A", "T1.B", "T2.A", "T2.B", "T2.C");

        // Now try the right join version.
        result = server.doCommand(
                "SELECT * FROM test_sj_t1 t1 RIGHT JOIN test_sj_t2 t2 ON t1.a = t2.a",
                true);
        TupleLiteral[] expected2 = {
                new TupleLiteral(3, 30, 3, -1, -1),
                new TupleLiteral(3, 30, 3, 40, 300),
                new TupleLiteral(3, 0, 3, -1, -1),
                new TupleLiteral(3, 0, 3, 40, 300),
                new TupleLiteral(4, 40, 4, 30, 400),
                new TupleLiteral(5, 50, 5, 60, 500),
                new TupleLiteral(6, 60, 6, 50, 600),
                new TupleLiteral(7, 70, 7, 70, 700),
                new TupleLiteral(8, 80, 8, 80, 800),
                new TupleLiteral(null, null, 9, 100, 900),
                new TupleLiteral(null, null, 10, 90, 1000)
        };
        assert checkSizeResults(expected2, result);
        assert checkUnorderedResults(expected2, result);
        checkResultSchema(result, "T1.A", "T1.B", "T2.A", "T2.B", "T2.C");

        // Same left join, but with an additional column in the predicate
        result = server.doCommand(
                "SELECT * FROM test_sj_t1 t1 LEFT JOIN test_sj_t2 t2 ON t1.a = t2.a AND t1.b = t2.b",
                true);
        TupleLiteral[] expected3 = {
                new TupleLiteral(1, 10, null, null, null),
                new TupleLiteral(2, 20, null, null, null),
                new TupleLiteral(3, 30, null, null, null),
                new TupleLiteral(3, 0, null, null, null),
                new TupleLiteral(4, 40, null, null, null),
                new TupleLiteral(5, 50, null, null, null),
                new TupleLiteral(6, 60, null, null, null),
                new TupleLiteral(7, 70, 7, 70, 700),
                new TupleLiteral(8, 80, 8, 80, 800)
        };
        assert checkSizeResults(expected3, result);
        assert checkUnorderedResults(expected3, result);
        checkResultSchema(result, "T1.A", "T1.B", "T2.A", "T2.B", "T2.C");

        // Right join version
        result = server.doCommand(
                "SELECT * FROM test_sj_t1 t1 RIGHT JOIN test_sj_t2 t2 ON t1.a = t2.a AND t1.b = t2.b",
                true);
        TupleLiteral[] expected4 = {
                new TupleLiteral(null, null, 3, -1, -1),
                new TupleLiteral(null, null, 3, 40, 300),
                new TupleLiteral(null, null, 4, 30, 400),
                new TupleLiteral(null, null, 5, 60, 500),
                new TupleLiteral(null, null, 6, 50, 600),
                new TupleLiteral(7, 70, 7, 70, 700),
                new TupleLiteral(8, 80, 8, 80, 800),
                new TupleLiteral(null, null, 9, 100, 900),
                new TupleLiteral(null, null, 10, 90, 1000)
        };
        assert checkSizeResults(expected4, result);
        assert checkUnorderedResults(expected4, result);
        checkResultSchema(result, "T1.A", "T1.B", "T2.A", "T2.B", "T2.C");
    }
}
