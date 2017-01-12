package edu.caltech.test.nanodb.sql;


import org.testng.annotations.Test;

import edu.caltech.nanodb.expressions.TupleLiteral;
import edu.caltech.nanodb.server.CommandResult;
import edu.caltech.nanodb.server.NanoDBServer;


/**
 * This class exercises the database with some simple <tt>SELECT</tt>
 * statements against a single table, to see if simple selects and
 * predicates work properly.
 */
@Test
public class TestSelectProject extends SqlTestCase {

    public TestSelectProject() {
        super("setup_testSelectProject");
    }


    /**
     * This test performs some simple projects that reorder the columns,
     * to see if the queries produce the expected results.
     *
     * @throws Exception if any query parsing or execution issues occur.
     */
    public void testProjectReorderCols() throws Throwable {
        // Columns c, a
        TupleLiteral[] expected1 = {
            new TupleLiteral(  10, 1),
            new TupleLiteral(  20, 2),
            new TupleLiteral(  30, 3),
            new TupleLiteral(null, 4),
            new TupleLiteral(  40, 5),
            new TupleLiteral(  50, 6)
        };

        // Columns c, b
        TupleLiteral[] expected2 = {
            new TupleLiteral(  10,    "red"),
            new TupleLiteral(  20, "orange"),
            new TupleLiteral(  30,     null),
            new TupleLiteral(null,  "green"),
            new TupleLiteral(  40, "yellow"),
            new TupleLiteral(  50,   "blue")
        };

        CommandResult result;

        result = server.doCommand(
            "SELECT c, a FROM test_select_project", true);
        assert checkUnorderedResults(expected1, result);

        result = server.doCommand(
            "SELECT c, b FROM test_select_project", true);
        assert checkUnorderedResults(expected2, result);
    }


    /**
     * This test performs some simple projects that perform arithmetic on the
     * column values, to see if the queries produce the expected results.
     *
     * @throws Exception if any query parsing or execution issues occur.
     */
    public void testProjectMath() throws Throwable {
        // Columns a - 10 as am, c * 3 as cm
        TupleLiteral[] expected = {
            new TupleLiteral(-9,   30),
            new TupleLiteral(-8,   60),
            new TupleLiteral(-7,   90),
            new TupleLiteral(-6, null),
            new TupleLiteral(-5,  120),
            new TupleLiteral(-4,  150)
        };

        CommandResult result;

        result = server.doCommand(
            "SELECT a - 10 AS am, c * 3 AS cm FROM test_select_project", true);
        assert checkUnorderedResults(expected, result);
    }


    /**
     * This test performs some simple projects that perform arithmetic on the
     * column values, along with a select predicate, to see if the queries
     * produce the expected results.
     *
     * @throws Exception if any query parsing or execution issues occur.
     */
    public void testSelectProjectMath() throws Throwable {
        // Columns b, a - 10 as am, c * 3 as cm
        TupleLiteral[] expected = {
            new TupleLiteral(    null, -7,   90),
            new TupleLiteral("yellow", -5,  120)
        };

        CommandResult result;

        result = server.doCommand(
            "SELECT b, a - 10 AS am, c * 3 AS cm FROM test_select_project " +
            "WHERE a > 2 AND c < 45", true);
        assert checkUnorderedResults(expected, result);
    }
}
