package edu.caltech.test.nanodb.sql;


import java.io.File;
import java.io.IOException;
import java.io.InputStream;

import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.ObjectUtils;

import edu.caltech.nanodb.relations.ColumnInfo;
import edu.caltech.nanodb.relations.Schema;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import edu.caltech.nanodb.expressions.TupleComparator;
import edu.caltech.nanodb.expressions.TupleLiteral;
import edu.caltech.nanodb.relations.Tuple;
import edu.caltech.nanodb.server.CommandResult;
import edu.caltech.nanodb.server.NanoDBServer;
import edu.caltech.nanodb.storage.StorageManager;


/**
 * This base-class provides functionality common to all testing classes that
 * issue SQL and examine the results.
 *
 * @review (Donnie) This class' constructors shouldn't be public; it is
 *         intended to be subclassed, and only subclasses call the parent
 *         class constructors.  Alas, TestNG requires us to do this...
 */
@Test(enabled=false)
public class SqlTestCase {

    public static final String TEST_SQL_PROPS =
        "edu/caltech/test/nanodb/sql/test_sql.props";


    protected NanoDBServer server;


    private Properties testSQL;


    /**
     * The data directory to use for the test cases, separate from the standard
     * data directory.
     */
    private File testBaseDir;


    private String setupSQLPropName;



    public SqlTestCase(String setupSQLPropName) {
        this.setupSQLPropName = setupSQLPropName;
    }


    public SqlTestCase() {
        this(null);
    }


    @BeforeClass
    public void beforeClass() throws Exception {
        // Set up a separate testing data-directory so that we don't clobber
        // any existing data.
        testBaseDir = new File("test_datafiles");
        if (!testBaseDir.exists())
            testBaseDir.mkdirs();
        else
            FileUtils.cleanDirectory(testBaseDir);

        // Make sure the database server uses the testing base-directory, not
        // the normal base-directory.
        System.setProperty(StorageManager.PROP_BASEDIR,
            testBaseDir.getAbsolutePath());

        server = new NanoDBServer();
        server.startup();

        // Run the initialization SQL, if it has been specified.
        if (setupSQLPropName != null) {
            loadTestSQLProperties();
            String setupSQL = testSQL.getProperty(setupSQLPropName);
            if (setupSQL == null) {
                throw new IOException("Property " + setupSQLPropName +
                    " not specified in " + TEST_SQL_PROPS);
            }

            List<CommandResult> results = server.doCommands(setupSQL, false);
            for (CommandResult result : results) {
                if (result.failed()) {
                    throw new Exception("Setup:  command failed with error:  " +
                        result.getFailure(), result.getFailure());
                }
            }
        }
    }


    private void loadTestSQLProperties() throws IOException {
        InputStream is =
            getClass().getClassLoader().getResourceAsStream(TEST_SQL_PROPS);
        if (is == null)
            throw new IOException("Couldn't find resource " + TEST_SQL_PROPS);

        testSQL = new Properties();
        testSQL.load(is);
        is.close();
    }



    @AfterClass
    public void afterClass() {
        // Shut down the database server and clean up the testing base-directory.
        server.shutdown();

        // Try to clean up the testing directory.
        try {
            FileUtils.cleanDirectory(testBaseDir);
        }
        catch (IOException e) {
            System.out.println("Couldn't clean directory " + testBaseDir);
            e.printStackTrace();
        }
    }


    public CommandResult tryDoCommand(String command, boolean includeTuples)
        throws Exception {
        CommandResult result = server.doCommand(command, includeTuples);

        if (result.failed())
            throw result.getFailure();

        return result;
    }


    public CommandResult tryDoCommand(String command) throws Exception {
        return tryDoCommand(command, false);
    }


    public List<TupleLiteral> getResultTuples(String command) throws Exception {
        CommandResult result = tryDoCommand(command, true);
        return result.getTuples();
    }


    /**
     * <p>
     * This helper function examines two collections of tuples, the expected
     * tuples and the actual tuples, and returns <tt>true</tt> if they are the
     * same, regardless of order.
     * </p>
     * <p>
     * The implementation of this method is effectively to do a nested-loop
     * join between the two result-sets.  Thus, it doesn't scale well.
     * </p>
     *
     * @param expected An array of tuple-literals containing the expected values.
     * @param actual A list of the actual tuple values produced by the database.
     * @return true if the two collections are the same, regardless of order, or
     *         false if they are not the same.
     */
    public boolean sameResultsUnordered(TupleLiteral[] expected,
                                        List<TupleLiteral> actual) {

        if (expected.length != actual.size())
            return false;

        LinkedList<TupleLiteral> expectedList = new LinkedList<>();
        Collections.addAll(expectedList, expected);

        for (Tuple a : actual) {
            Iterator<TupleLiteral> iter = expectedList.iterator();

            boolean found = false;
            while (iter.hasNext()) {
                Tuple e = iter.next();
                if (TupleComparator.areTuplesEqual(e, a)) {
                    iter.remove();
                    found = true;
                    break;
                }
            }

            // We saw a tuple in the actual results that doesn't match anything
            // in the expected results.
            if (!found)
                return false;
        }

        // If we got here, all actual results matched expected results.
        return true;
    }


    /**
     * This method examines a result's schema to ensure that the number of
     * columns, and the names of the individual columns, match what is
     * expected.  The expected column names can either be specified as
     * "colName" (with an unspecified table name), or "tblName.colName"
     * (with the specified table name).  The method will assert-fail if the
     * actual schema doesn't have the expected number of columns, or the
     * expected column names.
     *
     * @param result the result-schema generated by the query
     * @param expected the array of column names expected in the result schema
     */
    public void checkResultSchema(CommandResult result, String... expected) {
        Schema schema = result.getSchema();

        assert schema.numColumns() == expected.length :
            "Expected number of columns " + expected.length +
            " doesn't match actual number of columns " + schema.numColumns();

        for (int i = 0; i < schema.numColumns(); i++) {
            String expectedName = expected[i];
            String tblName = null;
            String colName = expectedName;

            int idx = expectedName.indexOf('.');
            if (idx != -1) {
                tblName = expectedName.substring(0, idx);
                colName = expectedName.substring(idx + 1);
            }

            ColumnInfo colInfo = schema.getColumnInfo(i);
            assert ObjectUtils.equals(colInfo.getTableName(), tblName) :
                "Expected column " + i + " to have table-name " + tblName;
            assert ObjectUtils.equals(colInfo.getColumnName().getColumnName(), colName) :
                "Expected column " + i + " to have column-name " + colName;
        }
    }


    /**
     * This helper function examines a command's results against an expected
     * collection of tuples, and returns <tt>true</tt> if the result tuples
     * are the same, regardless of order.  This function checks that the
     * command didn't throw any exceptions, before calling the
     * {@link #sameResultsUnordered(TupleLiteral[], List)} method to compare
     * the results themselves.
     *
     * @param expected An array of tuple-literals containing the expected values.
     * @param result The command-result containing the actual tuple values
     *               produced by the database.
     *
     * @return true if the two collections are the same, regardless of order, or
     *         false if they are not the same.
     *
     * @throws Exception if an error occurred during command execution, as
     *         reported by the command result.
     */
    public boolean checkUnorderedResults(TupleLiteral[] expected,
        CommandResult result) throws Exception {
        if (result.failed())
            throw result.getFailure();

        if (sameResultsUnordered(expected, result.getTuples())) {
            return true;
        }
        else {
            System.out.println("Expected results:");
            for (TupleLiteral tup : expected) {
                System.out.println(" * " + tup.toString());
            }

            System.out.println("Actual results:");
            for (Tuple tup : result.getTuples()) {
                System.out.println(" * " + tup.toString());
            }

            return false;
        }
    }


    /**
     * This helper function examines two collections of tuples, the expected
     * tuples and the actual tuples, and returns <tt>true</tt> if they are the
     * same tuples and in the same order.
     *
     * @param expected An array of tuple-literals containing the expected values.
     * @param actual A list of the actual tuple values produced by the database.
     * @return true if the two collections are the same, and in the same order,
     *         or false otherwise.
     */
    public boolean sameResultsOrdered(TupleLiteral[] expected,
                                      List<TupleLiteral> actual) {

        if (expected.length != actual.size())
            return false;

        int i = 0;
        for (Tuple a : actual) {
            Tuple e = expected[i];

            // The expected and actual tuples don't match.
            if (!TupleComparator.areTuplesEqual(e, a, 0.0001)) {
                System.out.println(e + " (expected) and " + a + " (actual) don't match.");
                return false;
            }

            // Go on to the next tuple in the expected array.
            i++;
        }

        // If we got here, all actual results matched expected results, and they
        // were in the same order.
        return true;
    }


    /**
     * This helper function examines a command's results against an expected
     * collection of tuples, and returns <tt>true</tt> if the result tuples
     * are the same, and in the same order.  This function checks that the
     * command didn't throw any exceptions, before calling the
     * {@link #sameResultsOrdered(TupleLiteral[], List)} method to compare
     * the results themselves.
     *
     * @param expected An array of tuple-literals containing the expected values.
     * @param result The command-result containing the actual tuple values
     *               produced by the database.
     *
     * @return true if the two collections are the same, and in the same order,
     *         or false otherwise.
     *
     * @throws Exception if an error occurred during command execution, as
     *         reported by the command result.
     */
    public boolean checkOrderedResults(TupleLiteral[] expected,
                                       CommandResult result) throws Exception {
        if (result.failed())
            throw result.getFailure();

        if (sameResultsOrdered(expected, result.getTuples())) {
            return true;
        }
        else {
            System.out.println("Expected results:");
            for (TupleLiteral tup : expected) {
                System.out.println(" * " + tup.toString());
            }

            System.out.println("Actual results:");
            for (Tuple tup : result.getTuples()) {
                System.out.println(" * " + tup.toString());
            }

            return false;
        }
    }


    /**
     * This helper function examines a command's results against an expected
     * collection of tuples, and returns <tt>true</tt> if the number of result
     * tuples is the same as expected. This function checks that the
     * command didn't throw any exceptions, before checking the length.
     *
     * @param expected An array of tuple-literals containing the expected values.
     * @param result The command-result containing the actual tuple values
     *               produced by the database.
     *
     * @return true if the two collections are the same length, or false
     *         otherwise.
     *
     * @throws Exception if an error occurred during command execution, as
     *         reported by the command result.
     */
    public boolean checkSizeResults(TupleLiteral[] expected,
                                       CommandResult result) throws Exception {
        if (result.failed()) {
            throw result.getFailure();
        }

        assert expected.length == result.getTuples().size() :
            String.format("Expected result tuple-count %d doesn't match " +
                "actual tuple-count %d", expected.length, result.getTuples().size());

        return true;
    }


    /**
     * This helper function takes in the name of a test table, and returns
     * <tt>true</tt> if there is at least one tuple in that table. This is
     * done by issuing a SQL command to select all values from the table.
     *
     * @param tableName the name of the table to check.
     *
     * @throws Throwable if an error occurred during command execution, as
     *         reported by the command result.
     */
    public void testTableNotEmpty(String tableName) throws Throwable {
        CommandResult result;

        result = server.doCommand("SELECT * FROM " + tableName, true);
        assert result.getTuples().size() > 0;
    }


    /**
     * This helper function takes in an <tt>int</tt> value, and returns a
     * TupleLiteral containing just that <tt>int</tt> as a value. This is
     * done for convenience when writing tests. It is overloaded to also
     * accept an double.
     *
     * @param val The int to use as the value of the TupleLiteral.
     *
     * @return a TupleLiteral containing only the value passed in.
     */
    protected TupleLiteral createTupleFromNum(int val) {
        TupleLiteral tmp = new TupleLiteral();
        tmp.addValue(val);
        return tmp;
    }


    /**
     * This helper function takes in a <tt>double</tt> value, and returns a
     * TupleLiteral containing just that <tt>double</tt> as a value. This is
     * done for convenience when writing tests. It is overloaded to also
     * accept an int.
     *
     * @param val The double to use as the value of the TupleLiteral.
     *
     * @return a TupleLiteral containing only the value passed in.
     */
    protected TupleLiteral createTupleFromNum(double val) {
        TupleLiteral tmp = new TupleLiteral();
        tmp.addValue(val);
        return tmp;
    }


    /**
     * This helper function prints the tuples from a command's results
     * to System.out for debugging purposes. It assumes that these are
     * the results produced by the test. It is overloaded to accept an
     * array of tuples as well. This function checks that the command
     * didn't throw any exceptions, before checking the length.
     *
     * @param result The results intended to be printed.
     */
    protected void printTuples(CommandResult result) throws Exception {
        // TODO: return a string instead of printing directly.
        if (result.failed()) {
            throw result.getFailure();
        }

        System.out.print("-------result: ");
        List<TupleLiteral> tups = result.getTuples();
        System.out.println(tups.size() + " tuple(s).");
        for (TupleLiteral t : tups) {
            System.out.println(t);
        }
    }


    /**
     * This helper function prints the tuples in an array to System.out for
     * debugging purposes. It assumes that these are the expected results
     * created by the programmer.. It is overloaded to accept the results
     * from a command (a CommandResult object) as well.
     *
     * @param tups The array intended to be printed.
     */
    protected void printTuples(TupleLiteral [] tups) {
        // TODO: return a string instead of printing directly.
        System.out.print("--------expected: ");
        System.out.println(tups.length + " tuple(s).");
        for (TupleLiteral t : tups) {
            System.out.println(t);
        }
    }
}
