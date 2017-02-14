package edu.caltech.test.nanodb.sql;


import edu.caltech.nanodb.expressions.ExpressionException;
import org.testng.annotations.Test;

import edu.caltech.nanodb.commands.ExecutionException;
import edu.caltech.nanodb.expressions.TupleLiteral;
import edu.caltech.nanodb.server.CommandResult;


/**
 * This class exercises the database with some simple scalar subqueries, to
 * verify that the most basic functionality works.
 **/
@Test
public class TestScalarSubquery extends SqlTestCase {
    public TestScalarSubquery() {
        super("setup_testExists");
    }

    /**
     * This test checks that at least one value was successfully inserted into
     * each of the test tables.
     *
     * @throws Exception if any query parsing or execution issues occur.
     */
    public void testExistsTablesNotEmpty() throws Throwable {
        testTableNotEmpty("test_exists_1");
        testTableNotEmpty("test_exists_2");
    }

    /**
     * This method exercises a simple scalar subquery in a WHERE predicate.
     *
     * @throws Exception if any query parsing or execution issues occur.
     */
    public void testScalarSubqueryWhereClause() throws Throwable {
        CommandResult result;
        TupleLiteral[] expected1 = {
            createTupleFromNum(1),
            createTupleFromNum(2),
            createTupleFromNum(3),
            createTupleFromNum(4)
        };
        TupleLiteral[] expected2 = { };

        result = server.doCommand(
            "SELECT a FROM test_exists_1 WHERE " +
                "(SELECT b FROM test_exists_2 WHERE b = 40) = 40", true);
        assert checkSizeResults(expected1, result);
        assert checkUnorderedResults(expected1, result);

        result = server.doCommand(
            "SELECT a FROM test_exists_1 WHERE " +
                "(SELECT b FROM test_exists_2 WHERE b = 50) = 40", true);
        assert checkSizeResults(expected2, result);
        assert checkUnorderedResults(expected2, result);
    }


    /**
     * This method exercises a simple scalar subquery in a SELECT predicate.
     *
     * @throws Exception if any query parsing or execution issues occur.
     */
    public void testScalarSubquerySelectClause() throws Throwable {
        CommandResult result;
        TupleLiteral[] expected = { new TupleLiteral( 4, 30 ) };

        result = server.doCommand(
            "SELECT (SELECT MAX(a) FROM test_exists_1) AS a, " +
            "       (SELECT MIN(b) FROM test_exists_2) AS b", true);

        printTuples(result);

        assert checkSizeResults(expected, result);
        assert checkUnorderedResults(expected, result);
    }


    /**
     * This method executes a scalar subquery that returns too many columns.
     *
     * @throws Exception if any query parsing or execution issues occur.
     */
    public void testScalarSubqueryTooManyCols() throws Throwable {
        CommandResult result;

        result = server.doCommand(
            "SELECT * FROM test_exists_1 " +
            "WHERE (SELECT b, b + 5 AS c FROM test_exists_2 WHERE b = 50)", true);

        assert result.failed();

        Exception e = result.getFailure();
        assert e.getClass().equals(ExecutionException.class);
        assert e.getCause().getClass().equals(ExpressionException.class);
    }


    /**
     * This method executes a scalar subquery that returns too few rows.
     *
     * @throws Exception if any query parsing or execution issues occur.
     */
    public void testScalarSubqueryTooFewRows() throws Throwable {
        CommandResult result;

        result = server.doCommand(
            "SELECT * FROM test_exists_1 " +
            "WHERE (SELECT b FROM test_exists_2 WHERE b > 100)", true);

        assert result.failed();

        Exception e = result.getFailure();
        assert e.getClass().equals(ExecutionException.class);
        assert e.getCause().getClass().equals(ExpressionException.class);
    }


    /**
     * This method executes a scalar subquery that returns too many rows.
     *
     * @throws Exception if any query parsing or execution issues occur.
     */
    public void testScalarSubqueryTooManyRows() throws Throwable {
        CommandResult result;

        result = server.doCommand(
            "SELECT * FROM test_exists_1 " +
            "WHERE (SELECT b FROM test_exists_2)", true);

        assert result.failed();

        Exception e = result.getFailure();
        assert e.getClass().equals(ExecutionException.class);
        assert e.getCause().getClass().equals(ExpressionException.class);
    }
}
