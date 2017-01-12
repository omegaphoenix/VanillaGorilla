package edu.caltech.nanodb.storage;

import java.io.IOException;
import java.util.List;

import edu.caltech.nanodb.expressions.Expression;
import edu.caltech.nanodb.relations.Tuple;


/**
 * This interface extends the {@link TupleFile} interface, adding operations
 * that can be provided on files of tuples that are hashed on a specific key.
 * The key used to hash tuples is returned by the {@link #getKeySpec} method.
 */
public interface HashedTupleFile extends TupleFile {
    /**
     * Returns the column(s) that comprise the hash key in this tuple file.
     *
     * @return the column(s) that comprise the hash key in this tuple file.
     */
    List<Expression> getKeySpec();


    /**
     * Returns the first tuple in the file that has the same hash-key values,
     * or {@code null} if there are no tuples with this hash-key value in
     * the tuple file.
     *
     * @param hashKey the tuple to search for
     *
     * @return The first tuple in the file with the same hash-key values, or
     *         {@code null} if the file contains no files with the specified
     *         search key value.  This tuple will actually be backed by the
     *         tuple file, so typically it will be a subclass of
     *         {@link PageTuple}.
     *
     * @throws IOException if an IO error occurs during the operation
     */
    Tuple findFirstTupleEquals(Tuple hashKey) throws IOException;


    /**
     * Returns the next entry in the index that has the same hash-key value,
     * or {@code null} if there are no more entries with this hash-key value
     * in the tuple file.
     *
     * @param prevTuple The tuple from which to resume the search for the next
     *        tuple with the same hash-key values.  This should be a tuple
     *        returned by a previous call to {@link #findFirstTupleEquals} or
     *        {@link #findNextTupleEquals}; using any other tuple would be an
     *        error.
     *
     * @return The next tuple in the file with the same hash-key values, or
     *         {@code null} if there are no more entries with this hash-key
     *         value in the file.
     *
     * @throws IOException if an IO error occurs during the operation
     */
    Tuple findNextTupleEquals(Tuple prevTuple) throws IOException;
}
