package edu.caltech.test.nanodb.sql;

import org.testng.annotations.Test;

import edu.caltech.nanodb.expressions.TupleComparator;
import edu.caltech.nanodb.expressions.TupleLiteral;
import edu.caltech.nanodb.server.CommandResult;
import edu.caltech.nanodb.server.NanoDBServer;

/**
* This class exercises the database with some grouping statements against many
* different tables, to see if GROUP BY expressions work properly. Since no
* aggregation occurs, we just check to see if the correct columns are grouped
* and generated.
**/
@Test
public class TestGroupBy extends SqlTestCase {
    public TestGroupBy() {
        super("setup_testGroupBy");
    }

    /**
     * This test checks that at least one value was successfully inserted into
     * the test table.
     *
     * @throws Exception if any query parsing or execution issues occur.
     **/
    public void testGroupByTablesNotEmpty() throws Throwable {
        testTableNotEmpty("test_group_by_a");
        testTableNotEmpty("test_group_by_b");
        testTableNotEmpty("test_group_by_c");
        testTableNotEmpty("test_group_by_d");
    }

    /**
    * This test performs grouping with one integer column, to see if the query
    * produces the expected results.
    *
    * @throws Exception if any query parsing or execution issues occur.
    **/
    public void testGroupByOneIntegerColumn() throws Throwable {
        CommandResult result;

        result = server.doCommand(
            "SELECT a FROM test_group_by_a GROUP BY a", true);
        TupleLiteral[] expected1 = {
            createTupleFromNum( 1 ),
            createTupleFromNum( 2 ),
            createTupleFromNum( 3 ),
            createTupleFromNum( 7 )
        };
        assert checkSizeResults(expected1, result);
        assert checkUnorderedResults(expected1, result);

        result = server.doCommand(
            "SELECT b FROM test_group_by_a GROUP BY b", true);
        TupleLiteral[] expected2 = {
            createTupleFromNum( 2 ),
            createTupleFromNum( 4 ),
            createTupleFromNum( 6 ),
            createTupleFromNum( 7 )
        };
        assert checkSizeResults(expected2, result);
        assert checkUnorderedResults(expected2, result);

        result = server.doCommand(
            "SELECT c FROM test_group_by_a GROUP BY c", true);
        TupleLiteral[] expected3 = {
            createTupleFromNum( 1 ),
            createTupleFromNum( 2 ),
            createTupleFromNum( 3 ),
            createTupleFromNum( 4 ),
            createTupleFromNum( 5 ),
            createTupleFromNum( 6 ),
            createTupleFromNum( 7 ),
            createTupleFromNum( 9 )
        };
        assert checkSizeResults(expected3, result);
        assert checkUnorderedResults(expected3, result);

        result = server.doCommand(
            "SELECT a FROM test_group_by_b GROUP BY a", true);
        TupleLiteral[] expected4 = {
            createTupleFromNum( 1 ),
            createTupleFromNum( 2 ),
            createTupleFromNum( 5 ),
            createTupleFromNum( 6 ),
            createTupleFromNum( 73 )
        };
        assert checkSizeResults(expected4, result);
        assert checkUnorderedResults(expected4, result);

        result = server.doCommand(
            "SELECT c FROM test_group_by_b GROUP BY c", true);
        TupleLiteral[] expected5 = {
            createTupleFromNum( 29 ),
            createTupleFromNum( 40 ),
            createTupleFromNum( 65 ),
            createTupleFromNum( 123 ),
            createTupleFromNum( 293 ),
            createTupleFromNum( 1235 ),
            createTupleFromNum( 5723 )
        };
        assert checkSizeResults(expected5, result);
        assert checkUnorderedResults(expected5, result);
    }

    /**
    * This test performs grouping with one VARCHAR column, to see if the query
    * produces the expected results.
    *
    * @throws Exception if any query parsing or execution issues occur.
    **/
    public void testGroupByOneVarcharColumn() throws Throwable {
        CommandResult result;

        result = server.doCommand(
            "SELECT street FROM test_group_by_c GROUP BY street", true);
        TupleLiteral[] expected1 = {
            new TupleLiteral( "Alma" ),
            new TupleLiteral( "Bluff" ),
            new TupleLiteral( "Broad" ),
            new TupleLiteral( "Coolidge" ),
            new TupleLiteral( "East" ),
            new TupleLiteral( "Elmer" ),
            new TupleLiteral( "Ember" ),
            new TupleLiteral( "First" ),
            new TupleLiteral( "Garden" ),
            new TupleLiteral( "Grant" ),
            new TupleLiteral( "Green" ),
            new TupleLiteral( "Hidden Hills" ),
            new TupleLiteral( "Leslie" ),
            new TupleLiteral( "Main" ),
            new TupleLiteral( "Nassau" ),
            new TupleLiteral( "North" ),
            new TupleLiteral( "Park" ),
            new TupleLiteral( "Putnam" ),
            new TupleLiteral( "Safety" ),
            new TupleLiteral( "Sand Hill" ),
            new TupleLiteral( "Second" ),
            new TupleLiteral( "Senator" ),
            new TupleLiteral( "Shady Cove" ),
            new TupleLiteral( "Smithton" ),
            new TupleLiteral( "South" ),
            new TupleLiteral( "Spring" ),
            new TupleLiteral( "University" ),
            new TupleLiteral( "Walnut" ),
            new TupleLiteral( "Washington" ),
            new TupleLiteral( "Willow" ),
            new TupleLiteral( "Wilson" )
        };
        assert checkSizeResults(expected1, result);
        assert checkUnorderedResults(expected1, result);

        result = server.doCommand(
            "SELECT city FROM test_group_by_c GROUP BY city", true);
        TupleLiteral[] expected2 = {
            new TupleLiteral( "Allentown" ),
            new TupleLiteral( "Brooklyn" ),
            new TupleLiteral( "Concord" ),
            new TupleLiteral( "Hampton" ),
            new TupleLiteral( "Harrison" ),
            new TupleLiteral( "Lakewood" ),
            new TupleLiteral( "Orangeford" ),
            new TupleLiteral( "Palo Alto" ),
            new TupleLiteral( "Pittsfield" ),
            new TupleLiteral( "Princeton" ),
            new TupleLiteral( "Rye" ),
            new TupleLiteral( "Salt Lake" ),
            new TupleLiteral( "Springfield" ),
            new TupleLiteral( "Stamford" ),
            new TupleLiteral( "Woodside" )
        };
        assert checkSizeResults(expected2, result);
        assert checkUnorderedResults(expected2, result);
    }

    /**
    * This test performs grouping with two integer columns, to see if the query
    * produces the expected results.
    *
    * @throws Exception if any query parsing or execution issues occur.
    **/
    public void testGroupByTwoIntegerColumns() throws Throwable {
        CommandResult result;

        result = server.doCommand(
            "SELECT a, b FROM test_group_by_a GROUP BY a, b", true);
        TupleLiteral[] expected1 = {
            new TupleLiteral( 1 , 2 ),
            new TupleLiteral( 1 , 4 ),
            new TupleLiteral( 1 , 6 ),
            new TupleLiteral( 2 , 4 ),
            new TupleLiteral( 2 , 6 ),
            new TupleLiteral( 3 , 7 ),
            new TupleLiteral( 7 , 6 ),
            new TupleLiteral( 7 , 7 )
        };
        assert checkSizeResults(expected1, result);
        assert checkUnorderedResults(expected1, result);

        result = server.doCommand(
            "SELECT b, c FROM test_group_by_a GROUP BY b, c", true);
        TupleLiteral[] expected2 = {
            new TupleLiteral( 2 , 5 ),
            new TupleLiteral( 4 , 2 ),
            new TupleLiteral( 4 , 6 ),
            new TupleLiteral( 4 , 7 ),
            new TupleLiteral( 6 , 2 ),
            new TupleLiteral( 6 , 3 ),
            new TupleLiteral( 6 , 4 ),
            new TupleLiteral( 6 , 9 ),
            new TupleLiteral( 7 , 1 ),
            new TupleLiteral( 7 , 2 )
        };
        assert checkSizeResults(expected2, result);
        assert checkUnorderedResults(expected2, result);

        result = server.doCommand(
            "SELECT c, a FROM test_group_by_b GROUP BY c, a", true);
        TupleLiteral[] expected3 = {
            new TupleLiteral( 29 , 73 ),
            new TupleLiteral( 40 , 1 ),
            new TupleLiteral( 40 , 5 ),
            new TupleLiteral( 65 , 1 ),
            new TupleLiteral( 65 , 5 ),
            new TupleLiteral( 123 , 1 ),
            new TupleLiteral( 123 , 6 ),
            new TupleLiteral( 293 , 5 ),
            new TupleLiteral( 1235 , 2 ),
            new TupleLiteral( 5723 , 6 )
        };
        assert checkSizeResults(expected3, result);
        assert checkUnorderedResults(expected3, result);
    }

    /**
    * This test performs grouping with two different column types, to see if
    * the query produces the expected results.
    *
    * @throws Exception is any query parsing or execution issues occur.
    **/
    public void testGroupByTwoColumnTypes() throws Throwable {
        CommandResult result;

        result = server.doCommand(
            "SELECT a, b FROM test_group_by_b GROUP BY a, b", true);
        TupleLiteral[] expected1 = {
            new TupleLiteral( 1 , null ),
            new TupleLiteral( 1 , "blah" ),
            new TupleLiteral( 1 , "okay" ),
            new TupleLiteral( 2 , "okay" ),
            new TupleLiteral( 5 , null ),
            new TupleLiteral( 5 , "four" ),
            new TupleLiteral( 5 , "okay" ),
            new TupleLiteral( 5 , "spam" ),
            new TupleLiteral( 6 , "asdf" ),
            new TupleLiteral( 6 , "five" ),
            new TupleLiteral( 73 , "spam" )
        };
        assert checkSizeResults(expected1, result);
        assert checkUnorderedResults(expected1, result);

        result = server.doCommand(
            "SELECT city, street FROM test_group_by_c GROUP BY city, street", true);
        TupleLiteral[] expected2 = {
            new TupleLiteral( "Allentown" , "East" ),
            new TupleLiteral( "Allentown" , "Hidden Hills" ),
            new TupleLiteral( "Brooklyn" , "Grant" ),
            new TupleLiteral( "Brooklyn" , "Senator" ),
            new TupleLiteral( "Brooklyn" , "Willow" ),
            new TupleLiteral( "Concord" , "Bluff" ),
            new TupleLiteral( "Concord" , "Main" ),
            new TupleLiteral( "Hampton" , "Coolidge" ),
            new TupleLiteral( "Hampton" , "Garden" ),
            new TupleLiteral( "Harrison" , "Main" ),
            new TupleLiteral( "Lakewood" , "Elmer" ),
            new TupleLiteral( "Orangeford" , "First" ),
            new TupleLiteral( "Orangeford" , "Leslie" ),
            new TupleLiteral( "Orangeford" , "Second" ),
            new TupleLiteral( "Palo Alto" , "Alma" ),
            new TupleLiteral( "Palo Alto" , "First" ),
            new TupleLiteral( "Palo Alto" , "Shady Cove" ),
            new TupleLiteral( "Pittsfield" , "Park" ),
            new TupleLiteral( "Pittsfield" , "Spring" ),
            new TupleLiteral( "Princeton" , "Green" ),
            new TupleLiteral( "Princeton" , "Nassau" ),
            new TupleLiteral( "Rye" , "Broad" ),
            new TupleLiteral( "Rye" , "First" ),
            new TupleLiteral( "Rye" , "Main" ),
            new TupleLiteral( "Rye" , "North" ),
            new TupleLiteral( "Rye" , "Safety" ),
            new TupleLiteral( "Rye" , "South" ),
            new TupleLiteral( "Rye" , "Washington" ),
            new TupleLiteral( "Salt Lake" , "Grant" ),
            new TupleLiteral( "Salt Lake" , "Smithton" ),
            new TupleLiteral( "Salt Lake" , "University" ),
            new TupleLiteral( "Salt Lake" , "Willow" ),
            new TupleLiteral( "Springfield" , "Coolidge" ),
            new TupleLiteral( "Stamford" , "Ember" ),
            new TupleLiteral( "Stamford" , "Putnam" ),
            new TupleLiteral( "Stamford" , "Walnut" ),
            new TupleLiteral( "Stamford" , "Wilson" ),
            new TupleLiteral( "Woodside" , "Sand Hill" )
        };
        assert checkSizeResults(expected2, result);
        assert checkUnorderedResults(expected2, result);
    }

    /**
    * This test performs grouping with more than two columns, to see if the
    * query produces the expeted results.
    *
    * @throws Exception if any query parsing or execution issues occur.
    **/
    public void testGroupByMultipleColumns() throws Throwable {
        CommandResult result;

        result = server.doCommand(
            "SELECT a, b, c FROM test_group_by_d GROUP BY a, b, c", true);
        TupleLiteral[] expected1 = {
            new TupleLiteral( 1 , 23.5 , "bool" ),
            new TupleLiteral( 1 , 23.5 , "nope" ),
            new TupleLiteral( 1 , 56.2 , "bool" ),
            new TupleLiteral( 1 , 56.2 , "nope" ),
            new TupleLiteral( 2 , 23.8 , "short" ),
            new TupleLiteral( 2 , 56.3 , "double" ),
            new TupleLiteral( 2 , 56.3 , "long" ),
            new TupleLiteral( 3 , 23.5 , "int" ),
            new TupleLiteral( 3 , 67.7 , "float" ),
            new TupleLiteral( 3 , 67.7 , "long" )
        };
        assert checkSizeResults(expected1, result);
        assert checkUnorderedResults(expected1, result);

        result = server.doCommand(
            "SELECT a, b, c, d FROM test_group_by_d GROUP BY a, b, c, d", true);
        TupleLiteral[] expected2 = {
            new TupleLiteral( 1 , 23.5 , "bool" , 1 ),
            new TupleLiteral( 1 , 23.5 , "nope" , 2 ),
            new TupleLiteral( 1 , 56.2 , "bool" , 2 ),
            new TupleLiteral( 1 , 56.2 , "nope" , 3 ),
            new TupleLiteral( 1 , 56.2 , "nope" , 5 ),
            new TupleLiteral( 2 , 23.8 , "short" , 1 ),
            new TupleLiteral( 2 , 23.8 , "short" , 34 ),
            new TupleLiteral( 2 , 56.3 , "double" , 4 ),
            new TupleLiteral( 2 , 56.3 , "long" , 1 ),
            new TupleLiteral( 2 , 56.3 , "long" , 34 ),
            new TupleLiteral( 3 , 23.5 , "int" , 3 ),
            new TupleLiteral( 3 , 23.5 , "int" , 7 ),
            new TupleLiteral( 3 , 67.7 , "float" , 45 ),
            new TupleLiteral( 3 , 67.7 , "long" , 2 )
        };
        assert checkSizeResults(expected2, result);
        assert checkUnorderedResults(expected2, result);

        result = server.doCommand(
            "SELECT a, b, c, d, e FROM test_group_by_d GROUP BY a, b, c, d, e", true);
        TupleLiteral[] expected3 = {
            new TupleLiteral( 1 , 23.5 , "bool" , 1 , 4 ),
            new TupleLiteral( 1 , 23.5 , "bool" , 1 , 6 ),
            new TupleLiteral( 1 , 23.5 , "nope" , 2 , 6 ),
            new TupleLiteral( 1 , 56.2 , "bool" , 2 , 5 ),
            new TupleLiteral( 1 , 56.2 , "nope" , 3 , 7 ),
            new TupleLiteral( 1 , 56.2 , "nope" , 5 , 7 ),
            new TupleLiteral( 2 , 23.8 , "short" , 1 , 5 ),
            new TupleLiteral( 2 , 23.8 , "short" , 34 , 6 ),
            new TupleLiteral( 2 , 56.3 , "double" , 4 , 5 ),
            new TupleLiteral( 2 , 56.3 , "long" , 1 , 3 ),
            new TupleLiteral( 2 , 56.3 , "long" , 1 , 5 ),
            new TupleLiteral( 2 , 56.3 , "long" , 34 , 65 ),
            new TupleLiteral( 3 , 23.5 , "int" , 3 , 4 ),
            new TupleLiteral( 3 , 23.5 , "int" , 7 , 3 ),
            new TupleLiteral( 3 , 67.7 , "float" , 45 , 45 ),
            new TupleLiteral( 3 , 67.7 , "float" , 45 , 67 ),
            new TupleLiteral( 3 , 67.7 , "long" , 2 , 5 )
        };
        assert checkSizeResults(expected3, result);
        assert checkUnorderedResults(expected3, result);

        result = server.doCommand(
            "SELECT a, b FROM test_group_by_d GROUP BY b, a", true);
        TupleLiteral[] expected4 = {
            new TupleLiteral( 1 , 23.5 ),
            new TupleLiteral( 3 , 23.5 ),
            new TupleLiteral( 2 , 23.8 ),
            new TupleLiteral( 1 , 56.2 ),
            new TupleLiteral( 2 , 56.3 ),
            new TupleLiteral( 3 , 67.7 )
        };
        assert checkSizeResults(expected4, result);
        assert checkUnorderedResults(expected4, result);

        result = server.doCommand(
            "SELECT d, c FROM test_group_by_d GROUP BY d, c", true);
        TupleLiteral[] expected5 = {
            new TupleLiteral( 1 , "bool" ),
            new TupleLiteral( 1 , "long" ),
            new TupleLiteral( 1 , "short" ),
            new TupleLiteral( 2 , "bool" ),
            new TupleLiteral( 2 , "long" ),
            new TupleLiteral( 2 , "nope" ),
            new TupleLiteral( 3 , "int" ),
            new TupleLiteral( 3 , "nope" ),
            new TupleLiteral( 4 , "double" ),
            new TupleLiteral( 5 , "nope" ),
            new TupleLiteral( 7 , "int" ),
            new TupleLiteral( 34 , "long" ),
            new TupleLiteral( 34 , "short" ),
            new TupleLiteral( 45 , "float" )
        };
        assert checkSizeResults(expected5, result);
        assert checkUnorderedResults(expected5, result);

        result = server.doCommand(
            "SELECT c, d, a FROM test_group_by_d GROUP BY c, d, a", true);
        TupleLiteral[] expected6 = {
            new TupleLiteral( "bool" , 1 , 1 ),
            new TupleLiteral( "bool" , 2 , 1 ),
            new TupleLiteral( "double" , 4 , 2 ),
            new TupleLiteral( "float" , 45 , 3 ),
            new TupleLiteral( "int" , 3 , 3 ),
            new TupleLiteral( "int" , 7 , 3 ),
            new TupleLiteral( "long" , 1 , 2 ),
            new TupleLiteral( "long" , 2 , 3 ),
            new TupleLiteral( "long" , 34 , 2 ),
            new TupleLiteral( "nope" , 2 , 1 ),
            new TupleLiteral( "nope" , 3 , 1 ),
            new TupleLiteral( "nope" , 5 , 1 ),
            new TupleLiteral( "short" , 1 , 2 ),
            new TupleLiteral( "short" , 34 , 2 )
        };
        assert checkSizeResults(expected6, result);
        assert checkUnorderedResults(expected6, result);
    }
}
