package edu.caltech.nanodb.indexes;


import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;

import org.apache.log4j.Logger;

import edu.caltech.nanodb.expressions.TupleLiteral;
import edu.caltech.nanodb.relations.ColumnRefs;
import edu.caltech.nanodb.relations.KeyColumnRefs;
import edu.caltech.nanodb.relations.TableConstraintType;
import edu.caltech.nanodb.relations.TableInfo;
import edu.caltech.nanodb.relations.TableSchema;
import edu.caltech.nanodb.storage.DBFile;
import edu.caltech.nanodb.storage.DBFileType;
import edu.caltech.nanodb.storage.FileManager;
import edu.caltech.nanodb.storage.PageTuple;
import edu.caltech.nanodb.storage.TupleFile;
import edu.caltech.nanodb.storage.StorageManager;
import edu.caltech.nanodb.storage.TupleFileManager;


public class BasicIndexManager implements IndexManager {
    /** A logging object for reporting anything interesting that happens. */
    private static Logger logger = Logger.getLogger(BasicIndexManager.class);


    /**
     * The index manager uses the storage manager a lot, so it caches a
     * reference to the storage manager at initialization.
     */
    private StorageManager storageManager;

    private HashMap<String, IndexInfo> openIndexes;


    /**
     * Initializes the heap-file table manager.  This class shouldn't be
     * initialized directly, since the storage manager will initialize it when
     * necessary.
     *
     * @param storageManager the storage manager that is using this table manager
     *
     * @throws IllegalArgumentException if {@code storageManager} is {@code null}
     */
    public BasicIndexManager(StorageManager storageManager) {
        if (storageManager == null)
            throw new IllegalArgumentException("storageManager cannot be null");

        this.storageManager = storageManager;
        openIndexes = new HashMap<String, IndexInfo>();
    }


    /**
     * This method takes a table name and an index name, and returns a
     * filename string that specifies where the index's data is stored.
     *
     * @param tableName the name of the table that the index is on
     * @param indexName the name of the index to get the filename of
     *
     * @return the name of the file that holds the index's data
     */
    private String getIndexFileName(String tableName, String indexName) {
        return tableName + "_" + indexName + ".idx";
    }


    @Override
    public boolean indexExists(String tableName, String indexName)
        throws IOException {

        String idxFileName = getIndexFileName(tableName, indexName);
        FileManager fileManager = storageManager.getFileManager();

        return fileManager.fileExists(idxFileName);
    }


    @Override
    public IndexInfo addIndexToTable(TableInfo tableInfo,
        ColumnRefs indexColRefs) throws IOException {

        if (tableInfo == null)
            throw new IllegalArgumentException("tableInfo cannot be null");

        if (indexColRefs == null)
            throw new IllegalArgumentException("indexColRefs cannot be null");

        // Figure out the schema and other essential details of the index.

        String tableName = tableInfo.getTableName();
        TableSchema tableSchema = tableInfo.getSchema();

        // TODO:  Check if current columns already are an index for this table.
        //        (Not essential, but you don't want to be redundant.)
        //        (NOTE:  Also need to make sure the index types are the same.)
        /*
        getIndexesOnColumns(columnNames, / * exactMatch * / true);

        for (Map.Entry<String, ColumnIndexes> entry :
            tableSchema.getIndexes().entrySet()) {
            if (index.equalsColumns(entry.getValue())) {
                throw new ExecutionException(String.format(
                    "Could not create new index on table \"%s\". The table " +
                        "already has an index \"%s\" with the same columns " +
                        "in the same order.", tableName, entry.getKey()));
            }
        }
        */

        String indexName = indexColRefs.getIndexName();
        logger.debug(String.format("Creating an IndexInfo object " +
            "describing the new index %s on table %s.",
            indexName != null ? indexName : "[unnamed]", tableName));

        IndexInfo indexInfo = new IndexInfo(tableInfo, indexColRefs);
        if (indexName == null) {
            // This is an unnamed index.
            logger.debug("Creating the new unnamed index on disk.");
            createUnnamedIndex(indexInfo);
        }
        else {
            // This is a named index.
            logger.debug("Creating the new index " + indexName + " on disk.");
            createIndex(indexInfo, indexName);
        }

        logger.debug("Index created.  Index name is " +
            indexInfo.getIndexName() + ", and filename is " +
            indexInfo.getTupleFile().getDBFile());

        TableConstraintType constraintType = indexColRefs.getConstraintType();
        if (constraintType != null && constraintType.isUnique())
            tableSchema.addCandidateKey((KeyColumnRefs) indexColRefs);
        else
            tableSchema.addIndex(indexColRefs);

        // Write schema with new index to file
        storageManager.getTableManager().saveTableInfo(tableInfo);

        // Populate the new index from the table's tuples.
        populateIndex(tableInfo, indexInfo);

        return indexInfo;
    }


    private void populateIndex(TableInfo srcTableInfo, IndexInfo newIndexInfo)
        throws IOException {

        String tableName = srcTableInfo.getTableName();
        String indexName = newIndexInfo.getIndexName();

        ColumnRefs columnRefs = newIndexInfo.getTableColumnRefs();
        TableConstraintType constraintType = columnRefs.getConstraintType();
        boolean unique = (constraintType != null && constraintType.isUnique());

        logger.debug(String.format("Populating new index %s with existing " +
            "tuples in table %s.", indexName, tableName));

        TupleFile tableTupleFile = srcTableInfo.getTupleFile();
        TupleFile indexTupleFile = newIndexInfo.getTupleFile();

        // Traverse the tuples in the table, so we can populate the index.
        PageTuple curTuple = (PageTuple) tableTupleFile.getFirstTuple();
        while (curTuple != null) {
            TupleLiteral idxTup;
            if (unique) {
                // Check if the index already has a tuple with this value.
                // The tuple we generate for this shouldn't include a tuple-
                // pointer since we just want to see if the value is repeated.
                idxTup = IndexUtils.makeTableSearchKey(columnRefs, curTuple,
                    /* findExactTuple */ false);

                if (IndexUtils.findTupleInIndex(idxTup, indexTupleFile) != null) {
                    // Adding this row would violate the unique index.
                    throw new IllegalStateException("Unique index " +
                        "already contains a tuple with this value.");
                }
            }

            // Generate the "index-schema" version of the tuple from the
            // "table-schema" version of the tuple.
            idxTup = IndexUtils.makeTableSearchKey(columnRefs, curTuple,
                /* findExactTuple */ true);
            indexTupleFile.addTuple(idxTup);

            // Move on to the next tuple in the table file.
            curTuple = (PageTuple) tableTupleFile.getNextTuple(curTuple);
        }
    }


    /**
     * Creates a new index file with the index name, table name, and column list
     * specified in the passed-in <tt>IndexInfo</tt> object.  Additional
     * details such as the data file and the index manager are stored into the
     * passed-in <tt>IndexInfo</tt> object upon successful creation of the
     * new index.
     *
     * @param indexInfo This object is an in/out parameter.  It is used to
     *        specify the name and details of the new index being created.  When
     *        the index is successfully created, the object is updated with the
     *        actual file that the index is stored in.
     * @param indexName The name of the index created by the CREATE INDEX cmd.
     *        If there is no idxName, use the createUnnamedIndex method
     *        instead.
     *
     * @throws IOException if the file cannot be created, or if an error occurs
     *         while storing the initial index data.
     */
    @Override
    public void createIndex(IndexInfo indexInfo, String indexName)
        throws IOException {

        String idxFileName = getIndexFileName(indexInfo.getTableName(), indexName);

        // TODO:  the file type and page size should be specified in the
        //        IndexInfo object
        int pageSize = StorageManager.getCurrentPageSize();
        DBFileType type = DBFileType.BTREE_TUPLE_FILE;
        TupleFileManager tupleFileManager = storageManager.getTupleFileManager(type);

        // First, create a new DBFile that the tuple file will go into.
        FileManager fileManager = storageManager.getFileManager();
        DBFile dbFile = fileManager.createDBFile(idxFileName, type, pageSize);
        logger.debug("Created new DBFile for index " + indexName +
                     " at path " + dbFile.getDataFile());

        // Generate a schema based on the index information.
        TableSchema indexSchema = IndexUtils.makeIndexSchema(
            indexInfo.getTableInfo().getSchema(),
            indexInfo.getTableColumnRefs());

        // Now, initialize it to be a tuple file with the specified type and
        // schema.
        TupleFile tupleFile = tupleFileManager.createTupleFile(dbFile, indexSchema);
        indexInfo.setTupleFile(tupleFile);

        // Cache this index since it's now considered "open".
        openIndexes.put(indexInfo.getTableName(), indexInfo);
    }


    @Override
    public void createUnnamedIndex(IndexInfo indexInfo) throws IOException {

        File baseDir = storageManager.getBaseDir();
        String tableName = indexInfo.getTableName();
        TableConstraintType constraintType =
            indexInfo.getTableColumnRefs().getConstraintType();

        // Figure out an index name and filename for the unnamed index.
        // Primary keys are handled separately, since each table only has
        // one primary key.  All other indexes are named by concatenating a
        // prefix with a numeric value that makes the index's name unique.
        File f;
        String indexName, indexFilename;
        String prefix = getUnnamedIndexPrefix(indexInfo);
        if (constraintType == TableConstraintType.PRIMARY_KEY) {
            indexName = prefix;
            indexFilename = getIndexFileName(tableName, indexName);
            f = new File(baseDir, indexFilename);
            if (!f.createNewFile()) {
                throw new IOException("Couldn't create file " + f +
                                      " for primary-key index " + indexName);
            }
        }
        else {
            String pattern = prefix + "_%03d";
            int i = 0;
            do {
                i++;
                indexName = String.format(pattern, i);
                indexFilename = getIndexFileName(tableName, indexName);
                f = new File(baseDir, indexFilename);
            }
            while (!f.createNewFile());
        }

        indexInfo.setIndexName(indexName);

        // Delete the file so we can create it in the createIndex() call.
        // TODO:  This is REALLY gross.
        f.delete();
        createIndex(indexInfo, indexName);
    }


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
    @Override
    public IndexInfo openIndex(TableInfo tableInfo, String indexName)
        throws IOException {

        IndexInfo indexInfo;

        // If the index is already open, just return the cached information.
        String indexKey = tableInfo.getTableName() + "." + indexName;
        indexInfo = openIndexes.get(indexKey);
        if (indexInfo != null)
            return indexInfo;

        // Open the data file for the index; read out its type and page-size.

        String idxFileName = getIndexFileName(tableInfo.getTableName(), indexName);
        TupleFile tupleFile = storageManager.openTupleFile(idxFileName);

        TableSchema tableSchema = tableInfo.getSchema();
        ColumnRefs columnRefs = tableSchema.getIndex(indexName);

        indexInfo = new IndexInfo(tableInfo, columnRefs, tupleFile);

        // Cache this index since it's now considered "open".
        openIndexes.put(indexKey, indexInfo);

        // Defer to the appropriate index-manager to read in the remainder of
        // the details.
        loadIndexInfo(indexInfo);

        return indexInfo;
    }


    /**
     * This function generates the prefix of a name for an index with no
     * actual name specified.  Since indexes and other constraints don't
     * necessarily require names to be specified, we need some way to
     * generate these names.
     *
     * @param indexInfo the information describing the index to be named
     *
     * @return a string containing a prefix to use for naming the index.
     */
    public String getUnnamedIndexPrefix(IndexInfo indexInfo) {
        // Generate a prefix based on the contents of the IndexInfo object.
        ColumnRefs colRefs = indexInfo.getTableColumnRefs();
        TableConstraintType constraintType = colRefs.getConstraintType();

        if (constraintType == null)
            return "IDX_" + indexInfo.getTableName();

        switch (constraintType) {
        case PRIMARY_KEY:
            return "PK_" + indexInfo.getTableName();

        case UNIQUE:
            return "CK_" + indexInfo.getTableName();

        case FOREIGN_KEY:
            return "FK_" + indexInfo.getTableName();

        default:
            throw new IllegalArgumentException("Unrecognized constraint type " +
                constraintType);
        }
    }


    // Copy interface javadocs.
    @Override
    public void saveIndexInfo(IndexInfo indexInfo) throws IOException {
        String indexName = indexInfo.getIndexName();
        String tableName = indexInfo.getTableName();
        DBFile dbFile = indexInfo.getTupleFile().getDBFile();

        //Schema schema = indexInfo.getSchema();

        logger.info(String.format(
            "Initializing new index %s on table %s, stored at %s", indexName,
            tableName, dbFile));

        // TODO
    }


    /**
     * This method reads in the schema and other critical information for the
     * specified table.
     *
     * @throws IOException if an IO error occurs when attempting to load the
     *         table's schema and other details.
     */
    public void loadIndexInfo(IndexInfo idxFileInfo) throws IOException {
        // For now, we don't need to do anything in this method.
    }


    @Override
    public List<String> verifyIndex(IndexInfo idxFileInfo) throws IOException {
        throw new UnsupportedOperationException("NYI");
    }

    @Override
    public void analyzeIndex(IndexInfo indexInfo) throws IOException {
        throw new UnsupportedOperationException("NYI");
    }

    @Override
    public void optimizeIndex(IndexInfo idxFileInfo) throws IOException {
        throw new UnsupportedOperationException("NYI");
    }

    @Override
    public void closeIndex(IndexInfo indexInfo) throws IOException {
        throw new UnsupportedOperationException("NYI");
    }

    @Override
    public void dropIndex(TableInfo tableInfo, String indexName) throws IOException {
        throw new UnsupportedOperationException("NYI");
    }
}
