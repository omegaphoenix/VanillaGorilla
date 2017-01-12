package edu.caltech.test.nanodb.sql;

import org.testng.annotations.Test;

import java.util.List;

import edu.caltech.nanodb.expressions.TupleComparator;
import edu.caltech.nanodb.expressions.TupleLiteral;
import edu.caltech.nanodb.server.CommandResult;
import edu.caltech.nanodb.server.NanoDBServer;

/**
* This class exercises the database with some aggregation statements against
* many different tables, to see if aggregate expressions work properly. We are
* just testing aggregation without any grouping.
**/
@Test
public class TestAggregation extends SqlTestCase {
    public TestAggregation() {
        super("setup_testAggregation");
    }

    /**
     * This test checks that at least one value was successfully inserted into
     * the test table.
     *
     * @throws Exception if any query parsing or execution issues occur.
     **/
    public void testAggregationTableNotEmpty() throws Throwable {
        testTableNotEmpty("test_aggregate");
    }

    /**
    * This test performs summing, to see if the query produces the expected
    * results.
    *
    * @throws Exception if any query parsing or execution issues occur.
    **/
    public void testSum() throws Throwable {
        CommandResult result;

        result = server.doCommand(
            "SELECT SUM(balance) FROM test_aggregate", true);
        TupleLiteral[] expected1 = {
            createTupleFromNum( 36700480 )
        };

        assert checkSizeResults(expected1, result);
        assert checkOrderedResults(expected1, result);

        result = server.doCommand(
            "SELECT SUM(balance) FROM test_aggregate WHERE branch_name = 'North Town'", true);
        TupleLiteral[] expected2 = {
            createTupleFromNum( 3700000 )
        };
        assert checkSizeResults(expected2, result);
        assert checkOrderedResults(expected2, result);
    }

    /**
    * This test performs finding the minimum, to see if the query produces the
    * expected results.
    *
    * @throws Exception if any query parsing or execution issues occur.
    **/
    public void testMin() throws Throwable {
        CommandResult result;

        result = server.doCommand(
            "SELECT MIN(balance) FROM test_aggregate", true);
        TupleLiteral[] expected1 = {
            createTupleFromNum( 400000 )
        };

        assert checkSizeResults(expected1, result);
        assert checkOrderedResults(expected1, result);


        result = server.doCommand(
            "SELECT MIN(balance) FROM test_aggregate WHERE balance > 500000", true);
        TupleLiteral[] expected2 = {
            createTupleFromNum( 625000 )
        };

        assert checkSizeResults(expected2, result);
        assert checkOrderedResults(expected2, result);
    }

    /**
    * This test performs finding the maximum, to see if the query produces the
    * expected results.
    *
    * @throws Exception if any query parsing or execution issues occur.
    **/
    public void testMax() throws Throwable {
        CommandResult result;


        result = server.doCommand(
            "SELECT MAX(balance) FROM test_aggregate", true);
        TupleLiteral[] expected1 = {
            createTupleFromNum( 8000000 )
        };
        assert checkSizeResults(expected1, result);
        assert checkOrderedResults(expected1, result);

        result = server.doCommand(
            "SELECT MAX(balance) FROM test_aggregate WHERE branch_name = 'Brighton'", true);
        TupleLiteral[] expected2 = {
            createTupleFromNum( 7000000 )
        };
        assert checkSizeResults(expected2, result);
        assert checkOrderedResults(expected2, result);
    }

    public void testEquals() throws Throwable {
        TupleLiteral expected1 = createTupleFromNum( 8000000 );
        TupleLiteral expected2 = createTupleFromNum( 8000000 );

        assert TupleComparator.areTuplesEqual(expected1, expected2);
    }


    /**
    * This test performs finding the average, to see if the query produces the
    * expected results.
    *
    * @throws Exception if any query parsing or execution issues occur.
    **/
    public void testAverage() throws Throwable {
        CommandResult result;


        result = server.doCommand(
            "SELECT AVG(balance) FROM test_aggregate", true);
        TupleLiteral[] expected1 = {
            createTupleFromNum( 2158851.7647058824 )
        };
        assert checkSizeResults(expected1, result);
        assert checkOrderedResults(expected1, result);

        result = server.doCommand(
            "SELECT AVG(balance) FROM test_aggregate WHERE balance < 500000", true);
        TupleLiteral[] expected2 = {
            createTupleFromNum( 400160.0 )
        };
        assert checkSizeResults(expected2, result);
        assert checkOrderedResults(expected2, result);
    }

    /**
    * This test performs finding the variance, to see if the query produces
    * the expected results.
    *
    * @throws Exception if any query parsing or execution issues occur.
    **/
    public void testVariance() throws Throwable {
        CommandResult result;

        result = server.doCommand(
            "SELECT VARIANCE(balance) FROM test_aggregate", true);
        TupleLiteral[] expected1 = {
            createTupleFromNum( 5.0809257708733545E12 )
        };
        assert checkSizeResults(expected1, result);
        assert checkOrderedResults(expected1, result);

        result = server.doCommand(
            "SELECT VARIANCE(balance) FROM test_aggregate WHERE city = 'Palo Alto'", true);
        TupleLiteral[] expected2 = {
            createTupleFromNum( 1.6E11 )
        };
        assert checkSizeResults(expected2, result);
        assert checkOrderedResults(expected2, result);
    }

    /**
    * This test performs finding the standard deviation, to see if the query
    * produces the expected results.
    *
    * @throws Exception if any query parsing or execution issues occur.
    **/
    public void testStdDev() throws Throwable {
        CommandResult result;

        result = server.doCommand(
            "SELECT STDDEV(balance) FROM test_aggregate", true);
        TupleLiteral[] expected1 = {
            createTupleFromNum( 2254090.896763783 )
        };
        assert checkSizeResults(expected1, result);
        assert checkOrderedResults(expected1, result);

        result = server.doCommand(
            "SELECT STDDEV(balance) FROM test_aggregate WHERE city = 'Palo Alto'", true);
        TupleLiteral[] expected2 = {
            createTupleFromNum( 400000.0 )
        };

        assert checkSizeResults(expected2, result);
        assert checkOrderedResults(expected2, result);
    }

    /**
    * This test performs finding the count, to see if the query produces the
    * expected results.
    *
    * @throws Exception if any query parsing or execution issues occur.
    **/
    public void testCount() throws Throwable {
        CommandResult result;

        result = server.doCommand(
            "SELECT COUNT(balance) FROM test_aggregate", true);
        TupleLiteral[] expected1 = {
            createTupleFromNum( 17 )
        };
        assert checkSizeResults(expected1, result);
        assert checkOrderedResults(expected1, result);

        result = server.doCommand(
            "SELECT COUNT(balance) FROM test_aggregate WHERE city = 'Palo Alto' OR city = 'Horseneck'", true);
        TupleLiteral[] expected2 = {
            createTupleFromNum( 5 )
        };
        assert checkSizeResults(expected2, result);
        assert checkOrderedResults(expected2, result);
    }

    /**
    * This test performs finding the count of distinct values, to see if the
    * query produces the expected results.
    *
    * @throws Exception if any query parsing or execution issues occur.
    **/
    public void testCountDistinct() throws Throwable {
        CommandResult result;

        result = server.doCommand(
            "SELECT COUNT(DISTINCT balance) FROM test_aggregate", true);
        TupleLiteral[] expected1 = {
            createTupleFromNum( 16 )
        };
System.err.println("RESULT TUPLES = " + result.getTuples());
        assert checkSizeResults(expected1, result);
        assert checkOrderedResults(expected1, result);

        result = server.doCommand(
            "SELECT COUNT(DISTINCT balance) FROM test_aggregate WHERE balance < 500000 AND city = 'Palo Alto' OR city = 'Horseneck'", true);
        TupleLiteral[] expected2 = {
            createTupleFromNum( 3 )
        };
        assert checkSizeResults(expected1, result);
        assert checkOrderedResults(expected2, result);
    }

    /**
    * This test performs finding the count of a table (finding the number of
    * rows), to see if the query produces the expected results.
    *
    * @throws Exception if any query parsing or execution issues occur.
    **/
    public void testCountStar() throws Throwable {
        CommandResult result;

        result = server.doCommand(
            "SELECT COUNT(*) FROM test_aggregate", true);
        TupleLiteral[] expected1 = {
            createTupleFromNum( 17 )
        };
        assert checkSizeResults(expected1, result);
        assert checkOrderedResults(expected1, result);

        result = server.doCommand(
            "SELECT COUNT(*) FROM test_aggregate WHERE balance > 1234567", true);
        TupleLiteral[] expected2 = {
            createTupleFromNum( 8 )
        };
        assert checkSizeResults(expected2, result);
        assert checkOrderedResults(expected2, result);
    }
}
