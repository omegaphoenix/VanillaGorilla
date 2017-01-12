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
public class TestStringMatch extends SqlTestCase {

    public TestStringMatch() {
        super("setup_testStringMatch");
    }


    /**
     * Test the wildcard expression <tt>s LIKE %</tt>.
     *
     * @throws Throwable if any query parsing or execution issues occur.
     */
    public void testLikeWildcard() throws Throwable {
        TupleLiteral[] expected = {
            new TupleLiteral("apple"),
            new TupleLiteral("banana"),
            new TupleLiteral("blueberry"),
            new TupleLiteral("cherry"),
            new TupleLiteral("grape"),
            new TupleLiteral("mandarine"),
            new TupleLiteral("mango"),
            new TupleLiteral("nectarine"),
            new TupleLiteral("orange"),
            new TupleLiteral("papaya"),
            new TupleLiteral("peach"),
            new TupleLiteral("pear"),
            new TupleLiteral("plum"),
            new TupleLiteral("raspberry"),
            new TupleLiteral("strawberry"),
            new TupleLiteral("tangerine")
        };

        CommandResult result = server.doCommand(
            "SELECT * FROM test_string_match WHERE s LIKE '%'", true);
        assert checkUnorderedResults(expected, result);
    }

    /**
     * Test a <tt>LIKE</tt> wildcard that matches the prefixes of strings.
     *
     * @throws Throwable if any query parsing or execution issues occur.
     */
    public void testLikeWildcardPrefix() throws Throwable {
        TupleLiteral[] expected = {
            new TupleLiteral("mandarine"),
            new TupleLiteral("mango")
        };

        CommandResult result = server.doCommand(
            "SELECT * FROM test_string_match WHERE s LIKE 'man%'", true);
        assert checkUnorderedResults(expected, result);
    }


    /**
     * Test a <tt>LIKE</tt> wildcard that matches the suffixes of strings.
     *
     * @throws Throwable if any query parsing or execution issues occur.
     */
    public void testLikeWildcardSuffix() throws Throwable {
        TupleLiteral[] expected = {
            new TupleLiteral("blueberry"),
            new TupleLiteral("cherry"),
            new TupleLiteral("raspberry"),
            new TupleLiteral("strawberry")
        };

        CommandResult result = server.doCommand(
            "SELECT * FROM test_string_match WHERE s LIKE '%erry'", true);
        assert checkUnorderedResults(expected, result);
    }


    /**
     * Test a <tt>LIKE</tt> wildcard that matches an internal portion of
     * strings.
     *
     * @throws Throwable if any query parsing or execution issues occur.
     */
    public void testLikeWildcardInner() throws Throwable {
        TupleLiteral[] expected = {
            new TupleLiteral("mandarine"),
            new TupleLiteral("nectarine"),
            new TupleLiteral("pear")
        };

        CommandResult result = server.doCommand(
            "SELECT * FROM test_string_match WHERE s LIKE '%ar%'", true);
        assert checkUnorderedResults(expected, result);
    }


    /**
     * Test <tt>LIKE</tt> character-wildcard patterns.
     *
     * @throws Throwable if any query parsing or execution issues occur.
     */
    public void testLikeCharMatch() throws Throwable {
        TupleLiteral[] expected = {
            new TupleLiteral("banana")
        };

        CommandResult result;

        result = server.doCommand(
            "SELECT * FROM test_string_match WHERE s LIKE '_anana'", true);
        assert checkUnorderedResults(expected, result);

        result = server.doCommand(
            "SELECT * FROM test_string_match WHERE s LIKE '_a_ana'", true);
        assert checkUnorderedResults(expected, result);

        result = server.doCommand(
            "SELECT * FROM test_string_match WHERE s LIKE '__nana'", true);
        assert checkUnorderedResults(expected, result);

        result = server.doCommand(
            "SELECT * FROM test_string_match WHERE s LIKE 'ban_n_'", true);
        assert checkUnorderedResults(expected, result);

        result = server.doCommand(
            "SELECT * FROM test_string_match WHERE s LIKE 'bana__'", true);
        assert checkUnorderedResults(expected, result);
    }


    /**
     * Test a <tt>LIKE</tt> wildcard that matches the prefixes of strings.
     *
     * @throws Throwable if any query parsing or execution issues occur.
     */
    public void testLikeComplexMatch() throws Throwable {
        TupleLiteral[] expected1 = {
            new TupleLiteral("raspberry"),
            new TupleLiteral("strawberry"),
            new TupleLiteral("tangerine")
        };

        TupleLiteral[] expected2 = {
            new TupleLiteral("grape"),
            new TupleLiteral("orange")
        };

        CommandResult result;

        result = server.doCommand(
            "SELECT * FROM test_string_match WHERE s LIKE '%a%er%'", true);
        assert checkUnorderedResults(expected1, result);

        result = server.doCommand(
            "SELECT * FROM test_string_match WHERE s LIKE '_r%'", true);
        assert checkUnorderedResults(expected2, result);
    }

}
