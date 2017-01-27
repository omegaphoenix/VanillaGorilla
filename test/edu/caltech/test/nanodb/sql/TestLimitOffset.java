package edu.caltech.test.nanodb.sql;

import org.testng.annotations.Test;

import edu.caltech.nanodb.expressions.TupleLiteral;
import edu.caltech.nanodb.server.CommandResult;
import edu.caltech.nanodb.server.NanoDBServer;

/**
 * This class exercises the database with both limit and offset
 * statements, to see if both kinds of expressions can work properly with each
 * other.
 **/
@Test
public class TestLimitOffset extends SqlTestCase {
    public TestLimitOffset() {
        super ("setup_testLimitOffset");
    }

    public void testLimit() throws Throwable {
        CommandResult result;

        result = server.doCommand(
                "SELECT * FROM test_limit_offset LIMIT 5", true);
        // System.err.println("HAVING RESULTS = " + result.getTuples());
        TupleLiteral[] expected1 = {
                new TupleLiteral(0, 0, 0),
                new TupleLiteral(0, 0, 1),
                new TupleLiteral(1, 1, 1),
                new TupleLiteral(1, 2, 1),
                new TupleLiteral(0, 1, 3)
        };
        assert checkSizeResults(expected1, result);
        assert checkUnorderedResults(expected1, result);

        result = server.doCommand(
                "SELECT * FROM test_limit_offset OFFSET 5", true);
        TupleLiteral[] expected2 = {
            new TupleLiteral( 2, 5, 8 ),
            new TupleLiteral( 0, 3, 5 ),
            new TupleLiteral( 1, 1, 1 ),
            new TupleLiteral( 21, 5, 51 ),
            new TupleLiteral( 2, 41, 9 )
        };
        assert checkSizeResults(expected2, result);
        assert checkUnorderedResults(expected2, result);

        result = server.doCommand(
                "SELECT * FROM test_limit_offset OFFSET 15", true);
        TupleLiteral[] expected4 = {
        };
        assert checkSizeResults(expected4, result);
        assert checkUnorderedResults(expected4, result);

        result = server.doCommand(
                "SELECT * FROM test_limit_offset LIMIT 100", true);
        TupleLiteral[] expected5 = {
            new TupleLiteral( 0, 0, 0 ),
            new TupleLiteral( 0, 0, 1 ),
            new TupleLiteral( 1, 1, 1 ),
            new TupleLiteral( 1, 2, 1 ),
            new TupleLiteral( 0, 1, 3 ),
            new TupleLiteral( 2, 5, 8 ),
            new TupleLiteral( 0, 3, 5 ),
            new TupleLiteral( 1, 1, 1 ),
            new TupleLiteral( 21, 5, 51 ),
            new TupleLiteral( 2, 41, 9 )
        };
        assert checkSizeResults(expected5, result);
        assert checkUnorderedResults(expected5, result);

    }
}
