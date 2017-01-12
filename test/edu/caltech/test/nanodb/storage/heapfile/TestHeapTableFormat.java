package edu.caltech.test.nanodb.storage.heapfile;


import org.testng.annotations.Test;

import edu.caltech.nanodb.expressions.TupleLiteral;
import edu.caltech.nanodb.server.CommandResult;
import edu.caltech.test.nanodb.storage.TableFormatTestCase;


/**
 * This test class exercises basic capabilities of the heap tuple file, to be
 * sure that inserts, updates and deletes all work correctly.  Note that it
 * tests the tuple file format by creating a table of that format, and then
 * performing various SQL operations against it.  It doesn't test the
 * implementation class directly.
 */
@Test
public class TestHeapTableFormat extends TableFormatTestCase {

    /**
     * Inserts into a table file, where everything should stay within a single
     * data page.
     *
     * @throws Exception if an IO error occurs, or if the test fails.
     */
    public void testHeapTableOnePageInsert() throws Exception {
        tryDoCommand("CREATE TABLE heap_1p_ins (a INTEGER, b VARCHAR(20)) " +
            "PROPERTIES (storage = 'heap', pagesize = 4096);", false);

        insertRows("heap_1p_ins", 150, 200, 3, 20, /* ordered */ false,
                   /* delete */ false);
    }


    /**
     * Inserts and deletes from a table file, where everything should stay
     * within a single data page.
     *
     * @throws Exception if an IO error occurs, or if the test fails.
     */
    public void testHeapTableOnePageInsertDelete() throws Exception {
        tryDoCommand("CREATE TABLE heap_1p_insdel (a INTEGER, b VARCHAR(20)) " +
            "PROPERTIES (storage = 'heap', pagesize = 4096);", false);

        insertRows("heap_1p_insdel", 150, 200, 3, 20, /* ordered */ false,
                   /* delete */ true);
    }


    /**
     * Inserts into a table file, where everything should stay within about
     * ten data pages.
     *
     * @throws Exception if an IO error occurs, or if the test fails.
     */
    public void testHeapTableMultiPageInsert() throws Exception {
        tryDoCommand("CREATE TABLE heap_mp_ins (a INTEGER, b VARCHAR(50)) " +
            "PROPERTIES (storage = 'heap', pagesize = 4096);", false);

        // This should require around 10 pages.
        insertRows("heap_mp_ins", 1000, 200, 20, 50, /* ordered */ false,
                   /* delete */ false);
    }


    /**
     * Inserts and deletes from a table file, where everything should stay
     * within about ten data pages.
     *
     * @throws Exception if an IO error occurs, or if the test fails.
     */
    public void testHeapTableMultiPageInsertDelete() throws Exception {
        tryDoCommand("CREATE TABLE heap_mp_insdel (a INTEGER, b VARCHAR(50)) " +
            "PROPERTIES (storage = 'heap', pagesize = 4096);", false);

        // Not sure how many pages this will require, since tuples will be
        // deleted along the way.
        insertRows("heap_mp_insdel", 3000, 200, 20, 50, /* ordered */ false,
                   /* delete */ true);
    }


    /**
     * Inserts and then deletes a sequence of 10000 rows, so that we can
     * detect if header entries are leaked, or tuple data ranges are leaked.
     * At the end of the test, the table file should have nothing.
     *
     * @throws Exception if an IO error occurs, or if the test fails.
     */
    public void testInsertDeleteManyTimes() throws Exception {
        tryDoCommand("CREATE TABLE heap_insdel (a INTEGER, b VARCHAR(20)) " +
            "PROPERTIES (storage = 'heap', pagesize = 4096);");

        // Insert a row, then delete it immediately.
        // If this loop starts failing after a hundred or more iterations
        // then it could be that tuple data is not being reclaimed.  If it
        // starts failing after around a thousand iterations then it could be
        // that header data is not being reclaimed.
        for (int i = 0; i < 10000; i++) {
            tryDoCommand(String.format("INSERT INTO %s VALUES (%d, '%s');",
                "heap_insdel", i, makeRandomString(3, 20)));
            tryDoCommand(String.format("DELETE FROM %s WHERE a = %d;",
                "heap_insdel", i));
        }

        // Should have deleted everything.
        CommandResult result = tryDoCommand("SELECT * FROM heap_insdel;", true);
        assert result.getTuples().size() == 0;
    }


    /**
     * This test performs a sequence of updates, to fully exercise the
     * tuple-value updating code.
     *
     * @throws Exception if an IO error occurs, or if the test fails.
     */
    public void testUpdates() throws Exception {
        CommandResult result;
        TupleLiteral[] tuples = {
            new TupleLiteral(35, 521L, "abcd", 3.14, "goodbye", 2.71828f),
            new TupleLiteral(6177281, -405691L, "fghi", 6.28, "puce", 54.669f),
            new TupleLiteral(-403662, 928032810L, "qrstu", 965.2323,
                "alongerstring", -31.2115f)
        };

        tryDoCommand("CREATE TABLE heap_update (a INTEGER, b BIGINT, " +
            "c CHAR(7), d DOUBLE, e VARCHAR(20), f FLOAT);");

        tryDoCommand("INSERT INTO heap_update VALUES (35, 521, 'abcd', " +
            "3.14, 'goodbye', 2.71828);");

        tryDoCommand("INSERT INTO heap_update VALUES (6177281, -405691, " +
            "'fghi', 6.28, 'puce', 54.669);");

        tryDoCommand("INSERT INTO heap_update VALUES (-403662, 928032810, " +
            "'qrstu', 965.2323, 'alongerstring', -31.2115);");

        // Make sure the data went in properly.
        result = tryDoCommand("SELECT * FROM heap_update;", true);
        checkUnorderedResults(tuples, result);

        // Now, mutate a few things, and see if the results are still valid.

        // First, some fixed-size things.
        tryDoCommand("UPDATE heap_update SET b = 92281965 WHERE a = 6177281;");
        tryDoCommand("UPDATE heap_update SET c = 'm' WHERE a = 35;");
        tryDoCommand("UPDATE heap_update SET a = 771 WHERE b = 521;");

        tuples[1].setColumnValue(1, 92281965L);
        tuples[0].setColumnValue(2, "m");
        tuples[0].setColumnValue(0, 771);

        result = tryDoCommand("SELECT * FROM heap_update;", true);
        assert checkUnorderedResults(tuples, result);

        // Next, try modifying multiple fixed-size things at once.
        tryDoCommand("UPDATE heap_update SET d = 55.55, f = -43.21 WHERE a = 771;");
        tryDoCommand("UPDATE heap_update SET b = -43, c = 'q', d = -3.32 WHERE a = -403662;");

        tuples[0].setColumnValue(3, 55.55);
        tuples[0].setColumnValue(5, -43.21f);

        tuples[2].setColumnValue(1, -43L);
        tuples[2].setColumnValue(2, "q");
        tuples[2].setColumnValue(3, -3.32);

        result = tryDoCommand("SELECT * FROM heap_update;", true);
        assert checkUnorderedResults(tuples, result);

        // Next, try modifying variable-sized stuff and see what happens.
        tryDoCommand("UPDATE heap_update SET e = 'hello' WHERE a = 771;");
        tryDoCommand("UPDATE heap_update SET e = 'goodeveningsir' WHERE a = 6177281;");

        tuples[0].setColumnValue(4, "hello");
        tuples[1].setColumnValue(4, "goodeveningsir");

        result = tryDoCommand("SELECT * FROM heap_update;", true);
        assert checkUnorderedResults(tuples, result);

        // Next, try setting stuff to NULL and see what happens.
        tryDoCommand("UPDATE heap_update SET b = NULL WHERE a = 771;");
        tryDoCommand("UPDATE heap_update SET e = NULL WHERE a = 6177281;");
        tryDoCommand("UPDATE heap_update SET d = NULL, f = NULL WHERE a = -403662;");

        tuples[0].setColumnValue(1, null);
        tuples[1].setColumnValue(4, null);
        tuples[2].setColumnValue(3, null);
        tuples[2].setColumnValue(5, null);

        result = tryDoCommand("SELECT * FROM heap_update;", true);
        assert checkUnorderedResults(tuples, result);

        // Finally, try setting some NULL stuff to non-NULL values.
        tryDoCommand("UPDATE heap_update SET e = 'whyohwhy', b = NULL WHERE a = 6177281;");
        tryDoCommand("UPDATE heap_update SET d = 5.32, f = NULL WHERE a = -403662;");
        tryDoCommand("UPDATE heap_update SET c = NULL, b = 6543210 WHERE a = 771;");

        tuples[1].setColumnValue(4, "whyohwhy");
        tuples[1].setColumnValue(1, null);

        tuples[2].setColumnValue(3, 5.32);
        tuples[2].setColumnValue(5, null);  // no-op

        tuples[0].setColumnValue(2, null);
        tuples[0].setColumnValue(1, 6543210L);

        result = tryDoCommand("SELECT * FROM heap_update;", true);
        assert checkUnorderedResults(tuples, result);
    }
}
