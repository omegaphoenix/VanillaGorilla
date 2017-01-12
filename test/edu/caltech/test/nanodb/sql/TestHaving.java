package edu.caltech.test.nanodb.sql;

import org.testng.annotations.Test;

import edu.caltech.nanodb.expressions.TupleLiteral;
import edu.caltech.nanodb.server.CommandResult;
import edu.caltech.nanodb.server.NanoDBServer;

/**
* This class exercises the database with both grouping and aggregation
* statements, to see if both kinds of expressions can work properly with each
* other.
**/
@Test
public class TestHaving extends SqlTestCase {
    public TestHaving() {
        super ("setup_testHaving");
    }

    /**
    * This test checks that at least one value was successfully inserted into
    * the test table.
    *
    * @throws Exception if any query parsing or execution issues occur.
    */
    public void testHavingTableNotEmpty() throws Throwable {
        testTableNotEmpty("test_having");
    }

    /**
    * This tests grouping and aggregation with having, to see if the query
    * produces the expected results.
    *
    * @throws Exception if any query parsing or execution issues occur.
    */
    public void testHaving() throws Throwable {
        CommandResult result;

        result = server.doCommand(
            "SELECT a, MIN(b) FROM test_having GROUP BY a HAVING MIN(b) < 2000", true);
System.err.println("HAVING RESULTS = " + result.getTuples());
        TupleLiteral[] expected1 = {
            new TupleLiteral( 7 , 990 ),
            new TupleLiteral( 11 , 980 ),
            new TupleLiteral( 20 , 970 ),
            new TupleLiteral( 21 , 1380 ),
            new TupleLiteral( 26 , 40 ),
            new TupleLiteral( 28 , 1870 ),
            new TupleLiteral( 31 , 750 ),
            new TupleLiteral( 35 , 270 ),
            new TupleLiteral( 44 , 850 ),
            new TupleLiteral( 48 , 1170 ),
            new TupleLiteral( 51 , 1650 ),
            new TupleLiteral( 73 , 410 ),
            new TupleLiteral( 74 , 1970 ),
            new TupleLiteral( 79 , 660 ),
            new TupleLiteral( 82 , 1050 ),
            new TupleLiteral( 89 , 1120 )
        };
        assert checkSizeResults(expected1, result);
        assert checkUnorderedResults(expected1, result);

        result = server.doCommand(
            "SELECT a, c + MIN(b) FROM test_having GROUP BY a, c HAVING c + MIN(b) < 5000", true);
        TupleLiteral[] expected2 = {
            new TupleLiteral( 21 , 4220 ),
            new TupleLiteral( 28 , 2953 ),
            new TupleLiteral( 51 , 4381 ),
            new TupleLiteral( 74 , 3039 )
        };
        assert checkSizeResults(expected2, result);
        assert checkUnorderedResults(expected2, result);

        result = server.doCommand(
            "SELECT a, b, AVG(d) FROM test_having GROUP BY a, b HAVING AVG(d) < 10", true);
        TupleLiteral[] expected3 = {
            new TupleLiteral( 1 , 4070 , 6.3933414875716 ),
            new TupleLiteral( 1 , 7900 , 1.82092360458958 ),
            new TupleLiteral( 13 , 9640 , 5.55555448203933 ),
            new TupleLiteral( 21 , 8830 , 3.95928872912797 ),
            new TupleLiteral( 31 , 4600 , 9.89438579995444 ),
            new TupleLiteral( 40 , 7210 , 6.14231405095463 ),
            new TupleLiteral( 44 , 850 , 1.99812089343904 ),
            new TupleLiteral( 48 , 1170 , 3.2819072269383 ),
            new TupleLiteral( 50 , 6750 , 9.2148696964581 ),
            new TupleLiteral( 51 , 1650 , 3.42313535916323 ),
            new TupleLiteral( 78 , 5010 , 5.6716755782163 ),
            new TupleLiteral( 82 , 9600 , 2.53535929141699 ),
            new TupleLiteral( 93 , 2540 , 6.17315259136148 )
        };
        assert checkSizeResults(expected3, result);
        assert checkUnorderedResults(expected3, result);

        result = server.doCommand(
            "SELECT b, SUM(a) FROM test_having GROUP BY b HAVING SUM(a) > 100", true);
        TupleLiteral[] expected4 = {
            new TupleLiteral( 5280 , 136 ),
            new TupleLiteral( 5760 , 114 ),
            new TupleLiteral( 9600 , 136 )
        };
        assert checkSizeResults(expected4, result);
        assert checkUnorderedResults(expected4, result);

        result = server.doCommand(
            "SELECT b, MAX(c + d) FROM test_having GROUP BY b HAVING MAX(c + d) < 7000", true);
        TupleLiteral[] expected5 = {
            new TupleLiteral( 970 , 5351.67798510299 ),
            new TupleLiteral( 1120 , 5536.00545770757 ),
            new TupleLiteral( 1380 , 2862.49957099823 ),
            new TupleLiteral( 1650 , 2734.42313535916 ),
            new TupleLiteral( 1870 , 1114.26447339072 ),
            new TupleLiteral( 1970 , 1155.79955097636 ),
            new TupleLiteral( 2440 , 4191.04521660599 ),
            new TupleLiteral( 2910 , 4370.60998416415 ),
            new TupleLiteral( 3890 , 3037.08105829087 ),
            new TupleLiteral( 4070 , 3883.39334148757 ),
            new TupleLiteral( 4190 , 3278.61913223135 ),
            new TupleLiteral( 4220 , 4532.21824719709 ),
            new TupleLiteral( 4250 , 1458.124808239 ),
            new TupleLiteral( 4730 , 3423.66063135287 ),
            new TupleLiteral( 5240 , 3992.9968760928 ),
            new TupleLiteral( 6890 , 2570.28812040649 ),
            new TupleLiteral( 7900 , 1428.82092360459 ),
            new TupleLiteral( 8100 , 3185.71262153259 ),
            new TupleLiteral( 8690 , 2058.73638464449 ),
            new TupleLiteral( 8830 , 491.959288729128 ),
            new TupleLiteral( 9640 , 1981.55555448204 ),
            new TupleLiteral( 9930 , 6879.21908284038 )
        };
        assert checkSizeResults(expected5, result);
        assert checkUnorderedResults(expected5, result);
    }
}
