package edu.caltech.test.nanodb.sql;


import edu.caltech.nanodb.expressions.TupleLiteral;
import edu.caltech.nanodb.relations.Schema;
import edu.caltech.nanodb.server.CommandResult;
import org.testng.annotations.Test;


/**
 * This class performs some basic tests with NATURAL joins and joins with the
 * USING clause, to ensure that both joins and projections are done properly.
 * These tests aren't exhaustive; they serve as a smoke-test to verify the
 * basic behaviors.
 */
@Test
public class TestNaturalUsingJoins extends SqlTestCase {
    public TestNaturalUsingJoins() {
        super("setup_testNaturalUsingJoins");
    }

    /**
     * This test checks that at least one value was successfully inserted into
     * each of the test tables.
     *
     * @throws Exception if any query parsing or execution issues occur.
     **/
    public void testNaturalUsingTablesNotEmpty() throws Throwable {
        testTableNotEmpty("test_nuj_t1");
        testTableNotEmpty("test_nuj_t2");
        testTableNotEmpty("test_nuj_t3");
        testTableNotEmpty("test_nuj_t4");
    }

    /**
     * This test performs natural joins with two tables, verifying both the
     * schema and the data that is returned.
     *
     * @throws Exception if any query parsing or execution issues occur.
     **/
    public void testNaturalUsingJoinTwoTables() throws Throwable {
        CommandResult result;

        // NATURAL JOIN with only one common column:  A
        result = server.doCommand(
            "SELECT * FROM test_nuj_t1 t1 NATURAL JOIN test_nuj_t3 t3", true);
        TupleLiteral[] expected1 = {
            new TupleLiteral(2, 20, 200, 2000),
            new TupleLiteral(3, 30, 300, 3000),
            new TupleLiteral(5, 50, 500, 5000),
            new TupleLiteral(6, 60, 600, 6000),
            new TupleLiteral(8, 80, 800, 8000)
        };
        assert checkSizeResults(expected1, result);
        assert checkUnorderedResults(expected1, result);
        checkResultSchema(result, "A", "T1.B", "T3.C", "T3.D");

        // USING with only one common column:  A
        result = server.doCommand(
            "SELECT * FROM test_nuj_t1 t1 JOIN test_nuj_t3 t3 USING (a)", true);
        assert checkSizeResults(expected1, result);
        assert checkUnorderedResults(expected1, result);
        checkResultSchema(result, "A", "T1.B", "T3.C", "T3.D");

        // NATURAL JOIN with two common columns:  A and C
        result = server.doCommand(
            "SELECT * FROM test_nuj_t2 t2 NATURAL JOIN test_nuj_t3 t3", true);
        TupleLiteral[] expected2 = {
            new TupleLiteral(3, 40, 300, 3000),
            new TupleLiteral(5, 60, 500, 5000),
            new TupleLiteral(6, 50, 600, 6000),
            new TupleLiteral(8, 80, 800, 8000),
            new TupleLiteral(9, 100, 900, 9000)
        };
        assert checkSizeResults(expected2, result);
        assert checkUnorderedResults(expected2, result);
        checkResultSchema(result, "A", "C", "T2.B", "T3.D");

        // USING with both common columns:  A, C
        result = server.doCommand(
            "SELECT * FROM test_nuj_t1 t1 JOIN test_nuj_t3 t3 USING (a, c)", true);
        assert checkSizeResults(expected2, result);
        assert checkUnorderedResults(expected2, result);
        checkResultSchema(result, "A", "C", "T1.B", "T3.D");

        // USING with only one of the common columns:  C
        result = server.doCommand(
            "SELECT * FROM test_nuj_t2 t2 JOIN test_nuj_t3 t3 USING (c)", true);
        TupleLiteral[] expected3 = {
            new TupleLiteral(300, 3, 40, 3, 3000),
            new TupleLiteral(500, 5, 60, 5, 5000),
            new TupleLiteral(600, 6, 50, 6, 6000),
            new TupleLiteral(800, 8, 80, 8, 8000),
            new TupleLiteral(900, 9, 100, 9, 9000)
        };
        assert checkSizeResults(expected3, result);
        assert checkUnorderedResults(expected3, result);
        checkResultSchema(result, "C", "T2.A", "T2.B", "T3.A", "T3.D");
    }


    /**
     * This test performs natural joins with three or more tables, verifying
     * both the schema and the data that is returned.  Joining this many
     * tables ensures that joins will operate against columns without table
     * names.
     *
     * @throws Exception if any query parsing or execution issues occur.
     **/
    public void testNaturalJoinMultiTables() throws Throwable {
        CommandResult result;

        // NATURAL JOIN three tables with a common column name A.
        // The first two tables are joined first, which means there will be
        // a column "A" without a table name, joined to the third table.
        result = server.doCommand(
            "SELECT * FROM (test_nuj_t1 t1 NATURAL JOIN test_nuj_t4 t4) NATURAL JOIN test_nuj_t5 t5", true);
        TupleLiteral[] expected1 = {
            new TupleLiteral(2, 20, 200, 2000),
            new TupleLiteral(5, 50, 600, 5000),
            new TupleLiteral(6, 60, 500, 6000)
        };
        assert checkSizeResults(expected1, result);
        assert checkUnorderedResults(expected1, result);
        checkResultSchema(result, "A", "T1.B", "T4.C", "T5.D");

        // NATURAL JOIN three tables with a common column name A.
        // The last two tables are joined first, which means there will be
        // a column "A" without a table name, joined to the first table.
        result = server.doCommand(
            "SELECT * FROM test_nuj_t1 t1 NATURAL JOIN (test_nuj_t4 t4 NATURAL JOIN test_nuj_t5 t5)", true);
        assert checkSizeResults(expected1, result);
        assert checkUnorderedResults(expected1, result);
        checkResultSchema(result, "A", "T1.B", "T4.C", "T5.D");

        // NATURAL JOIN three tables with a common column name A.
        // The first two tables are joined first, which means there will be
        // a column "A" without a table name, joined to the third table.
        result = server.doCommand(
            "SELECT * FROM (test_nuj_t1 t1 NATURAL JOIN test_nuj_t4 t4) " +
            "NATURAL JOIN (test_nuj_t5 t5 NATURAL JOIN test_nuj_t6 t6)", true);
        TupleLiteral[] expected2 = {
            new TupleLiteral(2, 20, 200, 2000, 20000),
            new TupleLiteral(5, 50, 600, 5000, 60000),
            new TupleLiteral(6, 60, 500, 6000, 50000)
        };
        assert checkSizeResults(expected2, result);
        assert checkUnorderedResults(expected2, result);
        checkResultSchema(result, "A", "T1.B", "T4.C", "T5.D", "T6.E");
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

        // USING-join three tables with a common column name A.
        // The first two tables are joined first, which means there will be
        // a column "A" without a table name, joined to the third table.
        result = server.doCommand(
            "SELECT * FROM (test_nuj_t1 t1 JOIN test_nuj_t4 t4 USING (A)) JOIN test_nuj_t5 t5 USING (A)", true);
        TupleLiteral[] expected1 = {
            new TupleLiteral(2, 20, 200, 2000),
            new TupleLiteral(5, 50, 600, 5000),
            new TupleLiteral(6, 60, 500, 6000)
        };
        assert checkSizeResults(expected1, result);
        assert checkUnorderedResults(expected1, result);
        checkResultSchema(result, "A", "T1.B", "T4.C", "T5.D");

        // USING-join three tables with a common column name A.
        // The last two tables are joined first, which means there will be
        // a column "A" without a table name, joined to the first table.
        result = server.doCommand(
            "SELECT * FROM test_nuj_t1 t1 JOIN (test_nuj_t4 t4 JOIN test_nuj_t5 t5 USING (A)) USING (A)", true);
        assert checkSizeResults(expected1, result);
        assert checkUnorderedResults(expected1, result);
        checkResultSchema(result, "A", "T1.B", "T4.C", "T5.D");

        // USING-join three tables with a common column name A.
        // The first two tables are joined first, which means there will be
        // a column "A" without a table name, joined to the third table.
        result = server.doCommand(
            "SELECT * FROM (test_nuj_t1 t1 JOIN test_nuj_t4 t4 USING (A)) " +
                "JOIN (test_nuj_t5 t5 JOIN test_nuj_t6 t6 USING (A)) USING (A)", true);
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
