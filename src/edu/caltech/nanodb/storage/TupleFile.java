package edu.caltech.nanodb.storage;


import java.io.IOException;
import java.util.List;
import java.util.Map;

import edu.caltech.nanodb.queryeval.TableStats;
import edu.caltech.nanodb.relations.TableSchema;
import edu.caltech.nanodb.relations.Tuple;


/**
 * This interface defines the most basic operations that all files storing
 * tuples must support.  The operations include the ability to scan through
 * all tuples in the file, and to add, modify and delete tuples.
 */
public interface TupleFile {

    /**
     * Returns the manager class for this kind of tuple file.
     *
     * @return the manager class for this kind of tuple file.
     */
    TupleFileManager getManager();


    /**
     * Returns the {@code DBFile} object that this tuple file is stored in.
     *
     * @design (donnie) For tuple files that are comprised of multiple
     *         physical files, this file will be the top-level file that
     *         records details such as all the other files necessary for
     *         that storage format.
     *
     * @return the {@code DBFile} object that this tuple file is stored in.
     */
    DBFile getDBFile();


    /**
     * Returns the schema of tuples stored in this tuple file.
     *
     * @return the schema of tuples stored in this tuple file.
     */
    TableSchema getSchema();


    /**
     * Returns statistics describing the data in this tuple file.  Note that
     * there is no corresponding "set-stats" method, because the
     * {@link #analyze} method is the intended way of updating a tuple file's
     * statistics.
     *
     * @return statistics describing the data in this tuple file.
     */
    TableStats getStats();


    /**
     * Returns the first tuple in this table file, or <tt>null</tt> if there
     * are no tuples in the file.
     *
     * @return the first tuple, or <tt>null</tt> if the table is empty
     *
     * @throws IOException if an IO error occurs while trying to read out the
     *         first tuple
     */
    Tuple getFirstTuple() throws IOException;


    /**
     * Returns the tuple that follows the specified tuple, or {@code null} if
     * there are no more tuples in the file.  The implementation must operate
     * correctly regardless of whether the input tuple is pinned or unpinned.
     *
     * @param tuple the "previous" tuple in the table.  This tuple may be
     *        pinned or unpinned.
     *
     * @return the tuple following the previous tuple, or {@code null} if the
     *         previous tuple is the last one in the table
     *
     * @throws IOException if an IO error occurs while trying to retrieve the
     *         next tuple.
     */
    Tuple getNextTuple(Tuple tuple) throws IOException;


    /**
     * Returns the tuple corresponding to the specified file pointer.  This
     * method is used by other features in the database, such as indexes.
     *
     * @param fptr a file-pointer specifying the tuple to retrieve
     *
     * @return the tuple referenced by <tt>fptr</tt>
     *
     * @throws InvalidFilePointerException if the specified file-pointer
     *         doesn't actually point to a real tuple.
     *
     * @throws IOException if an IO error occurs while trying to retrieve the
     *         specified tuple.
     */
    Tuple getTuple(FilePointer fptr)
        throws InvalidFilePointerException, IOException;


    /**
     * Adds the specified tuple into the table file, returning a new object
     * corresponding to the actual tuple added to the table.
     *
     * @param tuple a tuple object containing the values to add to the table
     *
     * @return a tuple object actually backed by this table
     *
     * @throws IOException if an IO error occurs while trying to add the new
     *         tuple to the table.
     */
    Tuple addTuple(Tuple tuple) throws IOException;


    /**
     * Modifies the values in the specified tuple.
     *
     * @param tuple the tuple to modify in the table
     *
     * @param newValues a map containing the name/value pairs to use to update
     *        the tuple.  Values in this map will be coerced to the
     *        column-type of the specified columns.  Only the columns being
     *        modified need to be specified in this collection.
     *
     * @throws IOException if an IO error occurs while trying to modify the
     *         tuple's values.
     */
    void updateTuple(Tuple tuple, Map<String, Object> newValues)
        throws IOException;


    /**
     * Deletes the specified tuple from the table.
     *
     * @param tuple the tuple to delete from the table
     *
     * @throws IOException if an IO error occurs while trying to delete the
     *         tuple.
     */
    void deleteTuple(Tuple tuple) throws IOException;


    /**
     * Analyzes the tuple data in the file, updating the file's statistics.
     *
     * @throws IOException if an IO error occurs while analyzing the file or
     *         updating the statistics.
     */
    void analyze() throws IOException;


    /**
     * Verifies the tuple file's internal storage format, identifying any
     * potential structural errors in the file.  Errors are returned as a list
     * of error-message strings, each one reporting some error that was found.
     *
     * @return a list of error-message strings describing issues identified
     *         during verification.  The list will be empty if the file has no
     *         identifiable errors.
     *
     * @throws IOException if an IO error occurs during verification.
     */
    List<String> verify() throws IOException;


    /**
     * Optimizes the tuple file's layout or other characteristics to ensure
     * optimal performance and space usage.  Tuple file formats that don't
     * provide any optimization capabilities can simply return when this is
     * called.
     *
     * @throws IOException if an IO error occurs during optimization.
     */
    void optimize() throws IOException;
}
