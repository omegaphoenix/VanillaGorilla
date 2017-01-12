package edu.caltech.nanodb.indexes;


import java.io.IOException;
import java.util.List;

import edu.caltech.nanodb.relations.ColumnRefs;
import edu.caltech.nanodb.relations.TableConstraintType;
import edu.caltech.nanodb.relations.TableInfo;


/**
 * This interface specifies all operations that are necessary for supporting
 * indexes in NanoDB.  Indexes are implemented as
 * {@link edu.caltech.nanodb.storage.TupleFile tuple files} with a schema
 * containing the indexed columns, as well as a tuple-pointer column that
 * references the indexed tuples.  Therefore, many of the specific lookup
 * operations are provided directly by the underlying
 * {@link edu.caltech.nanodb.storage.TupleFile} implementation that exposes
 * these operations.
 */
public interface IndexManager {

    /**
     * A constant specifying the name of the column that will hold the table's
     * tuple pointer in an index file.
     */
    public static final String COLNAME_TUPLEPTR = "#TUPLE_PTR";


    boolean indexExists(String tableName, String indexName) throws IOException;


    IndexInfo addIndexToTable(TableInfo tableInfo, ColumnRefs indexColRefs)
        throws IOException;


    void createIndex(IndexInfo indexInfo, String indexName) throws IOException;


    void createUnnamedIndex(IndexInfo indexInfo) throws IOException;


    /**
     * This method opens the data file corresponding to the specified index
     * name and reads in the index's details.  If the index is already open
     * then the cached data is simply returned.
     *
     * @param tableInfo the table that the index is defined on
     *
     * @param indexName the name of the index to open.  Indexes are not
     *        referenced directly except by CREATE/ALTER/DROP INDEX statements,
     *        so these index names are stored in the table schema files, and are
     *        generally opened when the optimizer needs to know what indexes are
     *        available.
     *
     * @return an object representing the details of the open index
     *
     * @throws java.io.FileNotFoundException if no index-file exists for the
     *         index; in other words, it doesn't yet exist.
     *
     * @throws IOException if an IO error occurs when attempting to open the
     *         index.
     */
    IndexInfo openIndex(TableInfo tableInfo, String indexName) throws IOException;


    /**
     * This method loads the details for the specified index.
     *
     * @param indexInfo the index information object to populate.  When this
     *        is passed in, it only contains the index's name, the name of the
     *        table the index is specified on, and the opened database file to
     *        read the data from.
     *
     * @throws IOException if an IO error occurs when attempting to load the
     *         index's details.
     */
    void loadIndexInfo(IndexInfo indexInfo) throws IOException;


    /**
     * This method initializes a newly created index file, using the details
     * specified in the passed-in <tt>IndexInfo</tt> object.
     *
     * @param indexInfo This object is an in/out parameter.  It is used to
     *        specify the name and details of the new index being created.  When
     *        the index is successfully created, the object is updated with the
     *        actual file that the index's data is stored in.
     *
     * @throws IOException if the file cannot be created, or if an error occurs
     *         while storing the initial index data.
     */
    void saveIndexInfo(IndexInfo indexInfo) throws IOException;


    /**
     * This function allows an index to be verified for proper structure and
     * contents.
     *
     * @param indexInfo the index to verify
     *
     * @throws IOException if an IO error occurs while verifying the index
     *
     * @todo Should this be in this interface?
     */
    List<String> verifyIndex(IndexInfo indexInfo) throws IOException;


    void analyzeIndex(IndexInfo indexInfo) throws IOException;


    /**
     * This method performs whatever optimization is suitable for the specific
     * kind of index that is implemented.  For example, if the logical and
     * physical ordering of pages is widely different, this method can resolve
     * that kind of issue.
     *
     * @param indexInfo the index to optimize
     *
     * @throws IOException if an IO error occurs while optimizing the index
     *
     * @todo Should this be in this interface?
     */
    void optimizeIndex(IndexInfo indexInfo) throws IOException;


    void closeIndex(IndexInfo indexInfo) throws IOException;


    void dropIndex(TableInfo tableInfo, String indexName) throws IOException;
}
