package edu.caltech.test.nanodb.sql;


import org.testng.annotations.Test;

import edu.caltech.nanodb.expressions.TupleLiteral;
import edu.caltech.nanodb.server.CommandResult;
import edu.caltech.nanodb.server.NanoDBServer;


/**
 * This class exercises the database with various <tt>IN</tt> predicates.
 **/
@Test
public class TestInPredicates extends SqlTestCase {
    public TestInPredicates() {
        super("setup_testExists");
    }

    /**
     * This test checks that at least one value was successfully inserted into
     * each of the test tables.
     *
     * @throws Exception if any query parsing or execution issues occur.
     */
    public void testInPredicatesTablesNotEmpty() throws Throwable {
        testTableNotEmpty("test_exists_1");
        testTableNotEmpty("test_exists_2");
    }

    /**
     * This method tests whether the IN predicate works properly with
     * literal values.
     *
     * @throws Exception if any query parsing or execution issues occur.
     */
    public void testInValues() throws Throwable {
        CommandResult result;
        TupleLiteral[] expected1 = {
            createTupleFromNum(1),
            createTupleFromNum(3)
        };
        TupleLiteral[] expected2 = {
            createTupleFromNum(2),
            createTupleFromNum(4)
        };

        result = server.doCommand(
            "SELECT a FROM test_exists_1 WHERE " +
                "a IN (-1, 1, 3, 5)", true);
        printTuples(result);
        assert checkSizeResults(expected1, result);
        assert checkUnorderedResults(expected1, result);

        result = server.doCommand(
            "SELECT a FROM test_exists_1 WHERE " +
                "a NOT IN (-1, 1, 3, 5)", true);
        printTuples(result);
        assert checkSizeResults(expected2, result);
        assert checkUnorderedResults(expected2, result);
    }

    /**
     * This method tests whether the IN predicate works properly with
     * a subquery.
     *
     * @throws Exception if any query parsing or execution issues occur.
     */
    public void testInSubquery() throws Throwable {
        CommandResult result;
        TupleLiteral[] expected1 = {
            createTupleFromNum(3),
            createTupleFromNum(4)
        };
        TupleLiteral[] expected2 = {
            createTupleFromNum(1),
            createTupleFromNum(2)
        };

        result = server.doCommand(
            "SELECT a FROM test_exists_1 WHERE " +
                "a * 10 IN (SELECT b FROM test_exists_2)", true);
        assert checkSizeResults(expected1, result);
        assert checkUnorderedResults(expected1, result);

        result = server.doCommand(
            "SELECT a FROM test_exists_1 WHERE " +
                "a * 10 NOT IN (SELECT b FROM test_exists_2)", true);
        assert checkSizeResults(expected2, result);
        assert checkUnorderedResults(expected2, result);
    }
}
