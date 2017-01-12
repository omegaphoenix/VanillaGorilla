package edu.caltech.nanodb.relations;


import edu.caltech.nanodb.storage.FilePointer;
import edu.caltech.nanodb.storage.Pinnable;

/**
 * This interface provides the operations that can be performed with a tuple.
 * In relational database theory, a tuple is an ordered set of attribute-value
 * pairs, but in this implementation the tuple's data and its schema are kept
 * completely separate.  This tuple interface simply provides an index-accessed
 * collection of values; the schema would be represented separately using the
 * {@link Schema} class.
 * <p>
 * Different implementations of this interface store their data in different
 * places.  Some tuple implementations (e.g. subclasses of
 * {@link edu.caltech.nanodb.storage.PageTuple}) load and store values straight
 * out of a tuple file, and thus their data is backed by a buffer page that
 * can be written back to the filesystem.  Other tuples may exist entirely in
 * memory, with no corresponding back-end storage.
 * <p>
 * SQL data types are mapped to/from Java data types as follows:
 * <ul>
 *   <li><tt>TINYINT</tt> - <tt>byte</tt> (8 bit signed integer)</li>
 *   <li><tt>SMALLINT</tt> - <tt>short</tt> (16 bit signed integer)</li>
 *   <li>
 *     <tt>INTEGER</tt> (<tt>INT</tt>) - <tt>int</tt> (32 bit signed
 *     integer)
 *   </li>
 *   <li><tt>BIGINT</tt> - <tt>long</tt> (64 bit signed integer)</li>
 *   <li><tt>CHAR</tt> and <tt>VARCHAR</tt> - <tt>java.lang.String</tt></li>
 *   <li><tt>NUMERIC</tt> - <tt>java.math.BigDecimal</tt></li>
 * </ul>
 *
 * @see Schema
 */
public interface Tuple extends Pinnable {
    /**
     * Returns true if this tuple is backed by a disk page that must be kept
     * in memory as long as the tuple is in use.  Some tuple implementations
     * allocate memory to store their values, and are therefore not affected
     * if disk pages are evicted from the Buffer Manager.  Others are backed
     * by disk pages, and the disk page cannot be evicted until the tuple is
     * no longer being used.  In cases where a plan-node needs to hold onto a
     * tuple for a long time (e.g. for sorting or grouping), the plan node
     * should probably make a copy of disk-backed tuples, or materialize the
     * results, etc.
     *
     * @return {@code true} if the tuple is backed by a disk page, or
     *         {@code false} if the tuple's data is allocated in the memory
     *         heap.
     */
    boolean isDiskBacked();


    /**
     * Returns a count of the number of columns in the tuple.
     *
     * @return a count of the number of columns in the tuple.
     */
    int getColumnCount();


    /**
     * Returns <tt>true</tt> if the specified column's value is <tt>NULL</tt>.
     *
     * @param colIndex the index of the column to check for <tt>NULL</tt>ness.
     *
     * @return <tt>true</tt> if the specified column is <tt>NULL</tt>,
     *         <tt>false</tt> otherwise.
     */
    boolean isNullValue(int colIndex);


    /**
     * Returns the value of a column, or <tt>null</tt> if the column's SQL
     * value is <tt>NULL</tt>.
     *
     * @param colIndex the index of the column to retrieve the value for
     * @return the value of the column, or <tt>null</tt> if the column is
     *         <tt>NULL</tt>.
     */
    Object getColumnValue(int colIndex);


    /**
     * Sets the value of a column.  If <tt>null</tt> is passed, the column
     * is set to the SQL <tt>NULL</tt> value.
     *
     * @param colIndex the index of the column to set the value for
     * @param value the value to store for the column, or <tt>null</tt> if the
     *        column should be set to <tt>NULL</tt>.
     */
    void setColumnValue(int colIndex, Object value);


    /**
     * This method returns an external reference to the tuple, which can be
     * stored and used to look up this tuple.  The external reference is
     * represented as a file-pointer.  Implementations can throw an
     * {@link UnsupportedOperationException} if the kind of tuple doesn't
     * support an external reference.
     *
     * @return a file-pointer that can be used to look up this tuple
     *
     * @throws UnsupportedOperationException if this operation is unsupported
     */
    public FilePointer getExternalReference();
}
