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
public class TestGroupingAndAggregation extends SqlTestCase {
    public TestGroupingAndAggregation() {
        super ("setup_testGroupingAndAggregation");
    }
    
    /**
    * This test checks that at least one value was successfully inserted into
    * the test table.
    *
    * @throws Exception if any query parsing or execution issues occur.
    **/
    public void testTableNotEmpty() throws Throwable {
        testTableNotEmpty("test_group_aggregate_b");
    }
    
    /**
    * This tests simple aggregation with simple grouping, to see if the query
    * produces the expected results.
    * 
    * @throws Exception if any query parsing or execution issues occur.
    **/
    public void testSimpleGroupingAndAggregation() throws Throwable {
        CommandResult result;
        
        result = server.doCommand(
            "SELECT branch_name, MIN(balance) FROM test_group_aggregate_b GROUP BY branch_name", true);
        TupleLiteral[] expected1 = {
            new TupleLiteral( "Belldale" , 60 ),
            new TupleLiteral( "Bretton" , 410 ),
            new TupleLiteral( "Brighton" , 1700 ),
            new TupleLiteral( "Central" , 300 ),
            new TupleLiteral( "Deer Park" , 640 ),
            new TupleLiteral( "Downtown" , 200 ),
            new TupleLiteral( "Greenfield" , 190 ),
            new TupleLiteral( "Marks" , 620 ),
            new TupleLiteral( "Mianus" , 60 ),
            new TupleLiteral( "North Town" , 380 ),
            new TupleLiteral( "Perryridge" , 520 ),
            new TupleLiteral( "Pownal" , 10 ),
            new TupleLiteral( "Redwood" , 340 ),
            new TupleLiteral( "Rock Ridge" , 50 ),
            new TupleLiteral( "Round Hill" , 34000 ),
            new TupleLiteral( "Stonewell" , 580 )
        };
        assert checkSizeResults(expected1, result);
        assert checkUnorderedResults(expected1, result);
        
        result = server.doCommand(
            "SELECT branch_name, MAX(balance) FROM test_group_aggregate_b GROUP BY branch_name", true);
        TupleLiteral[] expected2 = {
            new TupleLiteral( "Belldale" , 67000 ),
            new TupleLiteral( "Bretton" , 91000 ),
            new TupleLiteral( "Brighton" , 24000 ),
            new TupleLiteral( "Central" , 2000 ),
            new TupleLiteral( "Deer Park" , 19000 ),
            new TupleLiteral( "Downtown" , 200 ),
            new TupleLiteral( "Greenfield" , 92000 ),
            new TupleLiteral( "Marks" , 69000 ),
            new TupleLiteral( "Mianus" , 74000 ),
            new TupleLiteral( "North Town" , 37000 ),
            new TupleLiteral( "Perryridge" , 82000 ),
            new TupleLiteral( "Pownal" , 91000 ),
            new TupleLiteral( "Redwood" , 8200 ),
            new TupleLiteral( "Rock Ridge" , 50000 ),
            new TupleLiteral( "Round Hill" , 34000 ),
            new TupleLiteral( "Stonewell" , 66000 )
        };
        assert checkSizeResults(expected2, result);
        assert checkUnorderedResults(expected2, result);
        
        result = server.doCommand(
            "SELECT branch_name, AVG(balance) FROM test_group_aggregate_b GROUP BY branch_name", true);
        TupleLiteral[] expected3 = {
            new TupleLiteral( "Belldale" , 25353.333333333332 ),
            new TupleLiteral( "Bretton" , 44352.5000 ),
            new TupleLiteral( "Brighton" , 8800.0000 ),
            new TupleLiteral( "Central" , 880.0000 ),
            new TupleLiteral( "Deer Park" , 8513.333333333334 ),
            new TupleLiteral( "Downtown" , 200.0000 ),
            new TupleLiteral( "Greenfield" , 67547.5000 ),
            new TupleLiteral( "Marks" , 17565.0000 ),
            new TupleLiteral( "Mianus" , 24786.666666666668 ),
            new TupleLiteral( "North Town" , 10075.0000 ),
            new TupleLiteral( "Perryridge" , 21797.5000 ),
            new TupleLiteral( "Pownal" , 31172.0000 ),
            new TupleLiteral( "Redwood" , 2178.0000 ),
            new TupleLiteral( "Rock Ridge" , 13652.0000 ),
            new TupleLiteral( "Round Hill" , 34000.0000 ),
            new TupleLiteral( "Stonewell" , 38316.0000 )
        };
        assert checkSizeResults(expected3, result);
        assert checkUnorderedResults(expected3, result);
        
        result = server.doCommand(
            "SELECT branch_name, COUNT(balance) FROM test_group_aggregate_b GROUP BY branch_name", true);
        TupleLiteral[] expected4 = {
            new TupleLiteral( "Belldale" , 3 ),
            new TupleLiteral( "Bretton" , 4 ),
            new TupleLiteral( "Brighton" , 4 ),
            new TupleLiteral( "Central" , 3 ),
            new TupleLiteral( "Deer Park" , 3 ),
            new TupleLiteral( "Downtown" , 1 ),
            new TupleLiteral( "Greenfield" , 4 ),
            new TupleLiteral( "Marks" , 6 ),
            new TupleLiteral( "Mianus" , 3 ),
            new TupleLiteral( "North Town" , 4 ),
            new TupleLiteral( "Perryridge" , 4 ),
            new TupleLiteral( "Pownal" , 5 ),
            new TupleLiteral( "Redwood" , 5 ),
            new TupleLiteral( "Rock Ridge" , 5 ),
            new TupleLiteral( "Round Hill" , 1 ),
            new TupleLiteral( "Stonewell" , 5 )
        };
        assert checkSizeResults(expected4, result);
        assert checkUnorderedResults(expected4, result);
        
        result = server.doCommand(
            "SELECT branch_name, SUM(balance) FROM test_group_aggregate_b GROUP BY branch_name", true);
        TupleLiteral[] expected5 = {
            new TupleLiteral( "Belldale" , 76060 ),
            new TupleLiteral( "Bretton" , 177410 ),
            new TupleLiteral( "Brighton" , 35200 ),
            new TupleLiteral( "Central" , 2640 ),
            new TupleLiteral( "Deer Park" , 25540 ),
            new TupleLiteral( "Downtown" , 200 ),
            new TupleLiteral( "Greenfield" , 270190 ),
            new TupleLiteral( "Marks" , 105390 ),
            new TupleLiteral( "Mianus" , 74360 ),
            new TupleLiteral( "North Town" , 40300 ),
            new TupleLiteral( "Perryridge" , 87190 ),
            new TupleLiteral( "Pownal" , 155860 ),
            new TupleLiteral( "Redwood" , 10890 ),
            new TupleLiteral( "Rock Ridge" , 68260 ),
            new TupleLiteral( "Round Hill" , 34000 ),
            new TupleLiteral( "Stonewell" , 191580 )
        };
        assert checkSizeResults(expected5, result);
        assert checkUnorderedResults(expected5, result);
        
        result = server.doCommand(
            "SELECT branch_name, STDDEV(balance) FROM test_group_aggregate_b GROUP BY branch_name", true);
        TupleLiteral[] expected6 = {
            new TupleLiteral( "Belldale" , 29673.94517455039 ),
            new TupleLiteral( "Bretton" , 33994.819734041834 ),
            new TupleLiteral( "Brighton" , 8894.661320140302 ),
            new TupleLiteral( "Central" , 792.1279357948857 ),
            new TupleLiteral( "Deer Park" , 7719.867586659473 ),
            new TupleLiteral( "Downtown" , 0.0 ),
            new TupleLiteral( "Greenfield" , 38914.576558790926 ),
            new TupleLiteral( "Marks" , 25432.970563162035 ),
            new TupleLiteral( "Mianus" , 34799.21965919479 ),
            new TupleLiteral( "North Town" , 15568.785276957224 ),
            new TupleLiteral( "Perryridge" , 34781.25671608201 ),
            new TupleLiteral( "Pownal" , 38778.5958487411 ),
            new TupleLiteral( "Redwood" , 3019.1018532007165 ),
            new TupleLiteral( "Rock Ridge" , 18547.426128711228 ),
            new TupleLiteral( "Round Hill" , 0.0 ),
            new TupleLiteral( "Stonewell" , 22648.43093902975 )
        };
        assert checkSizeResults(expected6, result);
        assert checkUnorderedResults(expected6, result);
        
        result = server.doCommand(
            "SELECT branch_name, VARIANCE(balance) FROM test_group_aggregate_b GROUP BY branch_name", true);
        TupleLiteral[] expected7 = {
            new TupleLiteral( "Belldale" , 8.805430222222223E8 ),
            new TupleLiteral( "Bretton" , 1.15564776875E9 ),
            new TupleLiteral( "Brighton" , 7.9115E7 ),
            new TupleLiteral( "Central" , 627466.6666666666 ),
            new TupleLiteral( "Deer Park" , 5.959635555555556E7 ),
            new TupleLiteral( "Downtown" , 0.0 ),
            new TupleLiteral( "Greenfield" , 1.51434426875E9 ),
            new TupleLiteral( "Marks" , 6.468359916666666E8 ),
            new TupleLiteral( "Mianus" , 1.2109856888888888E9 ),
            new TupleLiteral( "North Town" , 2.42387075E8 ),
            new TupleLiteral( "Perryridge" , 1.20973581875E9 ),
            new TupleLiteral( "Pownal" , 1.503779496E9 ),
            new TupleLiteral( "Redwood" , 9114976.0 ),
            new TupleLiteral( "Rock Ridge" , 3.44007016E8 ),
            new TupleLiteral( "Round Hill" , 0.0 ),
            new TupleLiteral( "Stonewell" , 5.12951424E8 )
        };
        assert checkSizeResults(expected7, result);
        assert checkUnorderedResults(expected7, result);
    }
    
    /**
    * This tests complex aggregation with simple grouping, to see if the query
    * produces the expected results.
    *
    * @throws Exception if any query parsing or execution issues occur.
    **/
    public void testComplexGroupingSimpleAggregation() throws Throwable {
        CommandResult result;
        
        result = server.doCommand(
            "SELECT branch_name, MIN(balance), COUNT(balance) FROM test_group_aggregate_b GROUP BY branch_name", true);
        TupleLiteral[] expected1 = {
            new TupleLiteral( "Belldale" , 60 , 3 ),
            new TupleLiteral( "Bretton" , 410 , 4 ),
            new TupleLiteral( "Brighton" , 1700 , 4 ),
            new TupleLiteral( "Central" , 300 , 3 ),
            new TupleLiteral( "Deer Park" , 640 , 3 ),
            new TupleLiteral( "Downtown" , 200 , 1 ),
            new TupleLiteral( "Greenfield" , 190 , 4 ),
            new TupleLiteral( "Marks" , 620 , 6 ),
            new TupleLiteral( "Mianus" , 60 , 3 ),
            new TupleLiteral( "North Town" , 380 , 4 ),
            new TupleLiteral( "Perryridge" , 520 , 4 ),
            new TupleLiteral( "Pownal" , 10 , 5 ),
            new TupleLiteral( "Redwood" , 340 , 5 ),
            new TupleLiteral( "Rock Ridge" , 50 , 5 ),
            new TupleLiteral( "Round Hill" , 34000 , 1 ),
            new TupleLiteral( "Stonewell" , 580 , 5 )
        };
        assert checkSizeResults(expected1, result);
        assert checkUnorderedResults(expected1, result);
        
        result = server.doCommand(
            "SELECT branch_name, MIN(balance), COUNT(balance), AVG(balance), STDDEV(balance), VARIANCE(balance) FROM test_group_aggregate_b GROUP BY branch_name", true);
        
        TupleLiteral[] expected2 = {
            new TupleLiteral( "Belldale" , 60 , 3 , 25353.333333333332 , 29673.94517455039 , 8.805430222222223E8 ),
            new TupleLiteral( "Bretton" , 410 , 4 , 44352.5 , 33994.819734041834 , 1.15564776875E9 ),
            new TupleLiteral( "Brighton" , 1700 , 4 , 8800.0 , 8894.661320140302 , 7.9115E7 ),
            new TupleLiteral( "Central" , 300 , 3 , 880.0 , 792.1279357948857 , 627466.6666666666 ),
            new TupleLiteral( "Deer Park" , 640 , 3 , 8513.333333333334 , 7719.867586659473 , 5.959635555555556E7 ),
            new TupleLiteral( "Downtown" , 200 , 1 , 200.0 , 0.0 , 0.0 ),
            new TupleLiteral( "Greenfield" , 190 , 4 , 67547.5 , 38914.576558790926 , 1.51434426875E9 ),
            new TupleLiteral( "Marks" , 620 , 6 , 17565.0 , 25432.970563162035 , 6.468359916666666E8 ),
            new TupleLiteral( "Mianus" , 60 , 3 , 24786.666666666668 , 34799.21965919479 , 1.2109856888888888E9 ),
            new TupleLiteral( "North Town" , 380 , 4 , 10075.0 , 15568.785276957224 , 2.42387075E8 ),
            new TupleLiteral( "Perryridge" , 520 , 4 , 21797.5 , 34781.25671608201 , 1.20973581875E9 ),
            new TupleLiteral( "Pownal" , 10 , 5 , 31172.0 , 38778.5958487411 , 1.503779496E9 ),
            new TupleLiteral( "Redwood" , 340 , 5 , 2178.0 , 3019.1018532007165 , 9114976.0 ),
            new TupleLiteral( "Rock Ridge" , 50 , 5 , 13652.0 , 18547.426128711228 , 3.44007016E8 ),
            new TupleLiteral( "Round Hill" , 34000 , 1 , 34000.0 , 0.0 , 0.0 ),
            new TupleLiteral( "Stonewell" , 580 , 5 , 38316.0 , 22648.43093902975 , 5.12951424E8 )
        };
        assert checkSizeResults(expected2, result);
        assert checkUnorderedResults(expected2, result);
        
        result = server.doCommand(
            "SELECT branch_name, MIN(balance), COUNT(number) FROM test_group_aggregate_b GROUP BY branch_name", true);
        TupleLiteral[] expected3 = {
            new TupleLiteral( "Belldale" , 60 , 3 ),
            new TupleLiteral( "Bretton" , 410 , 4 ),
            new TupleLiteral( "Brighton" , 1700 , 4 ),
            new TupleLiteral( "Central" , 300 , 3 ),
            new TupleLiteral( "Deer Park" , 640 , 3 ),
            new TupleLiteral( "Downtown" , 200 , 1 ),
            new TupleLiteral( "Greenfield" , 190 , 4 ),
            new TupleLiteral( "Marks" , 620 , 6 ),
            new TupleLiteral( "Mianus" , 60 , 3 ),
            new TupleLiteral( "North Town" , 380 , 4 ),
            new TupleLiteral( "Perryridge" , 520 , 4 ),
            new TupleLiteral( "Pownal" , 10 , 5 ),
            new TupleLiteral( "Redwood" , 340 , 5 ),
            new TupleLiteral( "Rock Ridge" , 50 , 5 ),
            new TupleLiteral( "Round Hill" , 34000 , 1 ),
            new TupleLiteral( "Stonewell" , 580 , 5 )
        };
        assert checkSizeResults(expected3, result);
        assert checkUnorderedResults(expected3, result);
        
        result = server.doCommand(
            "SELECT MAX(balance), AVG(balance), COUNT(number) FROM test_group_aggregate_b GROUP BY branch_name", true);
        TupleLiteral[] expected4 = {
            new TupleLiteral( 67000 , 25353.333333333332 , 3 ),
            new TupleLiteral( 91000 , 44352.5000 , 4 ),
            new TupleLiteral( 24000 , 8800.0000 , 4 ),
            new TupleLiteral( 2000 , 880.0000 , 3 ),
            new TupleLiteral( 19000, 8513.333333333334, 3 ),
            new TupleLiteral( 200 , 200.0000 , 1 ),
            new TupleLiteral( 92000 , 67547.5000 , 4 ),
            new TupleLiteral( 69000 , 17565.0000 , 6 ),
            new TupleLiteral( 74000 , 24786.666666666668 , 3 ),
            new TupleLiteral( 37000 , 10075.0000 , 4 ),
            new TupleLiteral( 82000 , 21797.5000 , 4 ),
            new TupleLiteral( 91000 , 31172.0000 , 5 ),
            new TupleLiteral( 8200 , 2178.0000 , 5 ),
            new TupleLiteral( 50000 , 13652.0000 , 5 ),
            new TupleLiteral( 34000 , 34000.0000 , 1 ),
            new TupleLiteral( 66000 , 38316.0000 , 5 )
        };
        assert checkSizeResults(expected4, result);
        assert checkUnorderedResults(expected4, result);
    }
    
    /**
    * This tests simple and complex aggregation with multiple groups, to see if
    * the query produces the expected results.
    *
    * @throws Exception if any query parsing or execution issues occur.
    **/
    public void testAggregationMultipleGroups() throws Throwable {
        CommandResult result;
        
        result = server.doCommand(
            "SELECT branch_name, MIN(balance) FROM test_group_aggregate_b GROUP BY branch_name, number", true);
        TupleLiteral[] expected1 = {
            new TupleLiteral( "Belldale" , 67000 ),
            new TupleLiteral( "Belldale" , 9000 ),
            new TupleLiteral( "Belldale" , 60 ),
            new TupleLiteral( "Bretton" , 410 ),
            new TupleLiteral( "Bretton" , 27000 ),
            new TupleLiteral( "Bretton" , 91000 ),
            new TupleLiteral( "Bretton" , 59000 ),
            new TupleLiteral( "Brighton" , 1700 ),
            new TupleLiteral( "Brighton" , 3700 ),
            new TupleLiteral( "Brighton" , 24000 ),
            new TupleLiteral( "Brighton" , 5800 ),
            new TupleLiteral( "Central" , 300 ),
            new TupleLiteral( "Central" , 2000 ),
            new TupleLiteral( "Central" , 340 ),
            new TupleLiteral( "Deer Park" , 640 ),
            new TupleLiteral( "Deer Park" , 5900 ),
            new TupleLiteral( "Deer Park" , 19000 ),
            new TupleLiteral( "Downtown" , 200 ),
            new TupleLiteral( "Greenfield" , 92000 ),
            new TupleLiteral( "Greenfield" , 190 ),
            new TupleLiteral( "Greenfield" , 88000 ),
            new TupleLiteral( "Greenfield" , 90000 ),
            new TupleLiteral( "Marks" , 69000 ),
            new TupleLiteral( "Marks" , 870 ),
            new TupleLiteral( "Marks" , 1100 ),
            new TupleLiteral( "Marks" , 620 ),
            new TupleLiteral( "Marks" , 31000 ),
            new TupleLiteral( "Marks" , 2800 ),
            new TupleLiteral( "Mianus" , 300 ),
            new TupleLiteral( "Mianus" , 60 ),
            new TupleLiteral( "Mianus" , 74000 ),
            new TupleLiteral( "North Town" , 2500 ),
            new TupleLiteral( "North Town" , 380 ),
            new TupleLiteral( "North Town" , 37000 ),
            new TupleLiteral( "North Town" , 420 ),
            new TupleLiteral( "Perryridge" , 82000 ),
            new TupleLiteral( "Perryridge" , 520 ),
            new TupleLiteral( "Perryridge" , 3800 ),
            new TupleLiteral( "Perryridge" , 870 ),
            new TupleLiteral( "Pownal" , 64000 ),
            new TupleLiteral( "Pownal" , 470 ),
            new TupleLiteral( "Pownal" , 10 ),
            new TupleLiteral( "Pownal" , 91000 ),
            new TupleLiteral( "Pownal" , 380 ),
            new TupleLiteral( "Redwood" , 340 ),
            new TupleLiteral( "Redwood" , 560 ),
            new TupleLiteral( "Redwood" , 790 ),
            new TupleLiteral( "Redwood" , 1000 ),
            new TupleLiteral( "Redwood" , 8200 ),
            new TupleLiteral( "Rock Ridge" , 9500 ),
            new TupleLiteral( "Rock Ridge" , 7800 ),
            new TupleLiteral( "Rock Ridge" , 50 ),
            new TupleLiteral( "Rock Ridge" , 50000 ),
            new TupleLiteral( "Rock Ridge" , 910 ),
            new TupleLiteral( "Round Hill" , 34000 ),
            new TupleLiteral( "Stonewell" , 28000 ),
            new TupleLiteral( "Stonewell" , 43000 ),
            new TupleLiteral( "Stonewell" , 66000 ),
            new TupleLiteral( "Stonewell" , 580 ),
            new TupleLiteral( "Stonewell" , 54000 )
        };
        assert checkSizeResults(expected1, result);
        assert checkUnorderedResults(expected1, result);
        
        result = server.doCommand(
            "SELECT branch_name, MIN(balance), COUNT(number) FROM test_group_aggregate_b GROUP BY branch_name, number", true);
        TupleLiteral[] expected2 = {
            new TupleLiteral( "Belldale" , 67000 , 1 ),
            new TupleLiteral( "Belldale" , 9000 , 1 ),
            new TupleLiteral( "Belldale" , 60 , 1 ),
            new TupleLiteral( "Bretton" , 410 , 1 ),
            new TupleLiteral( "Bretton" , 27000 , 1 ),
            new TupleLiteral( "Bretton" , 91000 , 1 ),
            new TupleLiteral( "Bretton" , 59000 , 1 ),
            new TupleLiteral( "Brighton" , 1700 , 1 ),
            new TupleLiteral( "Brighton" , 3700 , 1 ),
            new TupleLiteral( "Brighton" , 24000 , 1 ),
            new TupleLiteral( "Brighton" , 5800 , 1 ),
            new TupleLiteral( "Central" , 300 , 1 ),
            new TupleLiteral( "Central" , 2000 , 1 ),
            new TupleLiteral( "Central" , 340 , 1 ),
            new TupleLiteral( "Deer Park" , 640 , 1 ),
            new TupleLiteral( "Deer Park" , 5900 , 1 ),
            new TupleLiteral( "Deer Park" , 19000 , 1 ),
            new TupleLiteral( "Downtown" , 200 , 1 ),
            new TupleLiteral( "Greenfield" , 92000 , 1 ),
            new TupleLiteral( "Greenfield" , 190 , 1 ),
            new TupleLiteral( "Greenfield" , 88000 , 1 ),
            new TupleLiteral( "Greenfield" , 90000 , 1 ),
            new TupleLiteral( "Marks" , 69000 , 1 ),
            new TupleLiteral( "Marks" , 870 , 1 ),
            new TupleLiteral( "Marks" , 1100 , 1 ),
            new TupleLiteral( "Marks" , 620 , 1 ),
            new TupleLiteral( "Marks" , 31000 , 1 ),
            new TupleLiteral( "Marks" , 2800 , 1 ),
            new TupleLiteral( "Mianus" , 300 , 1 ),
            new TupleLiteral( "Mianus" , 60 , 1 ),
            new TupleLiteral( "Mianus" , 74000 , 1 ),
            new TupleLiteral( "North Town" , 2500 , 1 ),
            new TupleLiteral( "North Town" , 380 , 1 ),
            new TupleLiteral( "North Town" , 37000 , 1 ),
            new TupleLiteral( "North Town" , 420 , 1 ),
            new TupleLiteral( "Perryridge" , 82000 , 1 ),
            new TupleLiteral( "Perryridge" , 520 , 1 ),
            new TupleLiteral( "Perryridge" , 3800 , 1 ),
            new TupleLiteral( "Perryridge" , 870 , 1 ),
            new TupleLiteral( "Pownal" , 64000 , 1 ),
            new TupleLiteral( "Pownal" , 470 , 1 ),
            new TupleLiteral( "Pownal" , 10 , 1 ),
            new TupleLiteral( "Pownal" , 91000 , 1 ),
            new TupleLiteral( "Pownal" , 380 , 1 ),
            new TupleLiteral( "Redwood" , 340 , 1 ),
            new TupleLiteral( "Redwood" , 560 , 1 ),
            new TupleLiteral( "Redwood" , 790 , 1 ),
            new TupleLiteral( "Redwood" , 1000 , 1 ),
            new TupleLiteral( "Redwood" , 8200 , 1 ),
            new TupleLiteral( "Rock Ridge" , 9500 , 1 ),
            new TupleLiteral( "Rock Ridge" , 7800 , 1 ),
            new TupleLiteral( "Rock Ridge" , 50 , 1 ),
            new TupleLiteral( "Rock Ridge" , 50000 , 1 ),
            new TupleLiteral( "Rock Ridge" , 910 , 1 ),
            new TupleLiteral( "Round Hill" , 34000 , 1 ),
            new TupleLiteral( "Stonewell" , 28000 , 1 ),
            new TupleLiteral( "Stonewell" , 43000 , 1 ),
            new TupleLiteral( "Stonewell" , 66000 , 1 ),
            new TupleLiteral( "Stonewell" , 580 , 1 ),
            new TupleLiteral( "Stonewell" , 54000 , 1 )
        };
        assert checkSizeResults(expected2, result);
        assert checkUnorderedResults(expected2, result);
    }
    
    /**
    * This tests grouping and aggregation with other kinds of SQL commands, to
    * see if the query produces the expected results.
    * 
    * @throws Exception if any query parsing or execution issues occur.
    **/
    public void testWithOtherCommands() throws Throwable {
        CommandResult result;
        
        result = server.doCommand(
            "SELECT branch_name, MAX(balance) FROM (SELECT branch_name, balance FROM test_group_aggregate_b) AS b GROUP BY branch_name", true);
        TupleLiteral[] expected1 = {
            new TupleLiteral( "Belldale" , 67000 ),
            new TupleLiteral( "Bretton" , 91000 ),
            new TupleLiteral( "Brighton" , 24000 ),
            new TupleLiteral( "Central" , 2000 ),
            new TupleLiteral( "Deer Park" , 19000 ),
            new TupleLiteral( "Downtown" , 200 ),
            new TupleLiteral( "Greenfield" , 92000 ),
            new TupleLiteral( "Marks" , 69000 ),
            new TupleLiteral( "Mianus" , 74000 ),
            new TupleLiteral( "North Town" , 37000 ),
            new TupleLiteral( "Perryridge" , 82000 ),
            new TupleLiteral( "Pownal" , 91000 ),
            new TupleLiteral( "Redwood" , 8200 ),
            new TupleLiteral( "Rock Ridge" , 50000 ),
            new TupleLiteral( "Round Hill" , 34000 ),
            new TupleLiteral( "Stonewell" , 66000 )
        };
        assert checkSizeResults(expected1, result);
        assert checkUnorderedResults(expected1, result);
        
        result = server.doCommand(
            "SELECT branch_name, MAX(balance) FROM test_group_aggregate_b GROUP BY branch_name ORDER BY branch_name", true);
        TupleLiteral[] expected2 = {
            new TupleLiteral( "Belldale" , 67000 ),
            new TupleLiteral( "Bretton" , 91000 ),
            new TupleLiteral( "Brighton" , 24000 ),
            new TupleLiteral( "Central" , 2000 ),
            new TupleLiteral( "Deer Park" , 19000 ),
            new TupleLiteral( "Downtown" , 200 ),
            new TupleLiteral( "Greenfield" , 92000 ),
            new TupleLiteral( "Marks" , 69000 ),
            new TupleLiteral( "Mianus" , 74000 ),
            new TupleLiteral( "North Town" , 37000 ),
            new TupleLiteral( "Perryridge" , 82000 ),
            new TupleLiteral( "Pownal" , 91000 ),
            new TupleLiteral( "Redwood" , 8200 ),
            new TupleLiteral( "Rock Ridge" , 50000 ),
            new TupleLiteral( "Round Hill" , 34000 ),
            new TupleLiteral( "Stonewell" , 66000 )
        };
        assert checkSizeResults(expected2, result);
        assert checkUnorderedResults(expected2, result);
    }
    
    
    /**
     * This tests complicated aggregation with aggregate functions along with
     * arithmetic expressions (i.e. SELECT a + MIN(b)) or SELECT MIN(a + b))
     *
     * @throws Exception if any query parsing or execution issues occur.
     */
    public void testComplicatedGroupingAndAggregation() throws Throwable {
        CommandResult result;
        
        result = server.doCommand(
            "SELECT a + MIN(b) FROM test_complicated_group_aggregation GROUP BY a", true);
        TupleLiteral[] expected = {
            new TupleLiteral(             24.5 ),
            new TupleLiteral( 25.7999992370605 ),
            new TupleLiteral(             26.5 )
        };
        assert checkSizeResults(expected, result);
        assert checkUnorderedResults(expected, result);
        
        
        result = server.doCommand(
            "SELECT c, a + SUM(b) FROM test_complicated_group_aggregation GROUP BY c, a", true);
        TupleLiteral[] expected2 = {
            new TupleLiteral( "bool"   , 104.200000762939 ),
            new TupleLiteral( "double" , 58.2999992370605 ),
            new TupleLiteral( "float"  , 138.399993896484 ),
            new TupleLiteral( "int"    ,               50 ),
            new TupleLiteral( "long"   , 170.899997711182 ),
            new TupleLiteral( "long"   , 70.6999969482422 ),
            new TupleLiteral( "nope"   , 136.900001525879 ),
            new TupleLiteral( "short"  , 49.5999984741211 )
        };
        assert checkSizeResults(expected2, result);
        assert checkUnorderedResults(expected2, result);
        
        
        result = server.doCommand(
            "SELECT c, a + AVG(b) + MIN(e) FROM test_complicated_group_aggregation GROUP BY c, a", true);
        TupleLiteral[] expected3 = {
            new TupleLiteral( "bool"   ,    39.4000002543131 ),
            new TupleLiteral( "double" ,    63.2999992370605 ),
            new TupleLiteral( "float"  ,    115.699996948242 ),
            new TupleLiteral( "int"    ,                29.5 ),
            new TupleLiteral( "long"   ,    61.2999992370605 ),
            new TupleLiteral( "long"   ,    75.6999969482422 ),
            new TupleLiteral( "nope"   ,    52.3000005086263 ),
            new TupleLiteral( "short"  ,    30.7999992370605 )
        };
        assert checkSizeResults(expected3, result);
        assert checkUnorderedResults(expected3, result);
        
        
        result = server.doCommand(
            "SELECT c, MIN(a + b) FROM test_complicated_group_aggregation GROUP BY c", true);
        TupleLiteral[] expected4 = {
            new TupleLiteral( "bool"   ,             24.5 ),
            new TupleLiteral( "double" , 58.2999992370605 ),
            new TupleLiteral( "float"  , 70.6999969482422 ),
            new TupleLiteral( "int"    ,             26.5 ),
            new TupleLiteral( "long"   , 58.2999992370605 ),
            new TupleLiteral( "nope"   ,             24.5 ),
            new TupleLiteral( "short"  , 25.7999992370605 )
        };
        assert checkSizeResults(expected4, result);
        assert checkUnorderedResults(expected4, result);
        
        
        result = server.doCommand(
            "SELECT c, MIN(a + e) + MAX(b) FROM test_complicated_group_aggregation GROUP BY c", true);
        TupleLiteral[] expected5 = {
            new TupleLiteral( "bool"   ,    61.2000007629395 ),
            new TupleLiteral( "double" ,    63.2999992370605 ),
            new TupleLiteral( "float"  ,    115.699996948242 ),
            new TupleLiteral( "int"    ,                29.5 ),
            new TupleLiteral( "long"   ,    72.6999969482422 ),
            new TupleLiteral( "nope"   ,    63.2000007629395 ),
            new TupleLiteral( "short"  ,    30.7999992370605 )
        };
        assert checkSizeResults(expected5, result);
        assert checkUnorderedResults(expected5, result);
        
        
        result = server.doCommand(
            "SELECT c, MIN(a) + AVG(e + b) FROM test_complicated_group_aggregation GROUP BY c", true);
        TupleLiteral[] expected6 = {
            new TupleLiteral( "bool"   ,    40.4000002543131 ),
            new TupleLiteral( "double" ,    63.2999992370605 ),
            new TupleLiteral( "float"  ,    126.699996948242 ),
            new TupleLiteral( "int"    ,                  30 ),
            new TupleLiteral( "long"   ,     80.649998664856 ),
            new TupleLiteral( "nope"   ,     52.966667175293 ),
            new TupleLiteral( "short"  ,    31.2999992370605 )
        };
        assert checkSizeResults(expected6, result);
        assert checkUnorderedResults(expected6, result);
    }
}
