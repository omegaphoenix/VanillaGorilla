package edu.caltech.nanodb.storage;


import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

import org.apache.log4j.Logger;

import edu.caltech.nanodb.commands.CommandProperties;
import edu.caltech.nanodb.indexes.IndexManager;
import edu.caltech.nanodb.relations.ForeignKeyColumnRefs;
import edu.caltech.nanodb.relations.KeyColumnRefs;
import edu.caltech.nanodb.relations.TableInfo;
import edu.caltech.nanodb.relations.TableSchema;


/**
 * This class provides an implementation of the {@link TableManager} interface
 * for tables that can have indexes and constraints on them.
 */
public class IndexedTableManager implements TableManager {

    /** A logging object for reporting anything interesting that happens. */
    private static Logger logger = Logger.getLogger(IndexedTableManager.class);


    private StorageManager storageManager;

    private HashMap<String, TableInfo> openTables;


    public IndexedTableManager(StorageManager storageManager) {
        this.storageManager = storageManager;
        openTables = new HashMap<String, TableInfo>();
    }

    /**
     * This method takes a table name and returns a filename string that
     * specifies where the table's data is stored.
     *
     * @param tableName the name of the table to get the filename of
     *
     * @return the name of the file that holds the table's data
     */
    private String getTableFileName(String tableName) {
        return tableName + ".tbl";
    }

    @Override
    public ArrayList<String> getTables() {
        ArrayList<String> tableNames = new ArrayList<String>();
        FileManager fileManager = storageManager.getFileManager();
        for(File dbFile : fileManager.getDBFiles()) {
            // Note that in getTableFileName all we do is add a .tbl at the end,
            // so that's all we have to account for.
            tableNames.add(dbFile.getName().replaceAll("\\.tbl$", ""));
        }
        return tableNames;
    }

    // Inherit interface docs.
    @Override
    public boolean tableExists(String tableName) throws IOException {
        String tblFileName = getTableFileName(tableName);
        FileManager fileManager = storageManager.getFileManager();

        return fileManager.fileExists(tblFileName);
    }


    // Inherit interface docs.
    @Override
    public TableInfo createTable(String tableName, TableSchema schema,
        CommandProperties properties) throws IOException {

        int pageSize = StorageManager.getCurrentPageSize();
        String storageType = "heap";

        if (properties != null) {
            logger.info("Using command properties " + properties);

            pageSize = properties.getInt("pagesize", pageSize);
            storageType = properties.getString("storage", storageType);

            HashSet<String> names = new HashSet<String>(properties.getNames());
            names.remove("pagesize");
            names.remove("storage");
            if (!names.isEmpty()) {
                throw new IllegalArgumentException("Unrecognized property " +
                    "name(s) specified:  " + names);
            }
        }

        String tblFileName = getTableFileName(tableName);

        DBFileType type;
        if ("heap".equals(storageType)) {
            type = DBFileType.HEAP_TUPLE_FILE;
        }
        else if ("btree".equals(storageType)) {
            type = DBFileType.BTREE_TUPLE_FILE;
        }
        else {
            throw new IllegalArgumentException("Unrecognized table file " +
                "type:  " + storageType);
        }
        TupleFileManager tupleFileManager = storageManager.getTupleFileManager(type);

        // First, create a new DBFile that the tuple file will go into.
        FileManager fileManager = storageManager.getFileManager();
        DBFile dbFile = fileManager.createDBFile(tblFileName, type, pageSize);
        logger.debug("Created new DBFile for table " + tableName +
                     " at path " + dbFile.getDataFile());

        // Now, initialize it to be a tuple file with the specified type and
        // schema.
        TupleFile tupleFile = tupleFileManager.createTupleFile(dbFile, schema);

        // Cache this table since it's now considered "open".
        TableInfo tableInfo = new TableInfo(tableName, tupleFile);
        openTables.put(tableName, tableInfo);

        return tableInfo;
    }


    // Inherit interface docs.
    @Override
    public void saveTableInfo(TableInfo tableInfo) throws IOException {
        TupleFile tupleFile = tableInfo.getTupleFile();
        TupleFileManager manager = tupleFile.getManager();
        manager.saveMetadata(tupleFile);
    }


    // Inherit interface docs.
    @Override
    public TableInfo openTable(String tableName) throws IOException {
        TableInfo tableInfo;

        // If the table is already open, just return the cached information.
        tableInfo = openTables.get(tableName);
        if (tableInfo != null)
            return tableInfo;

        // Open the data file for the table; read out its type and page-size.

        String tblFileName = getTableFileName(tableName);
        TupleFile tupleFile = storageManager.openTupleFile(tblFileName);
        tableInfo = new TableInfo(tableName, tupleFile);

        // Cache this table since it's now considered "open".
        openTables.put(tableName, tableInfo);

        return tableInfo;
    }


    // Inherit interface docs.
    @Override
    public void analyzeTable(TableInfo tableInfo) throws IOException {
        // Analyze the table's tuple-file.
        tableInfo.getTupleFile().analyze();

        // TODO:  Probably want to analyze all the indexes associated with
        //        the table as well...
    }


    // Inherit interface docs.
    @Override
    public void closeTable(TableInfo tableInfo) throws IOException {
        // Remove this table from the cache since it's about to be closed.
        openTables.remove(tableInfo.getTableName());

        DBFile dbFile = tableInfo.getTupleFile().getDBFile();

        // Flush all open pages for the table.
        storageManager.getBufferManager().flushDBFile(dbFile);
        storageManager.getFileManager().closeDBFile(dbFile);
    }


    // Inherit interface docs.
    @Override
    public void dropTable(String tableName) throws IOException {
        TableInfo tableInfo = openTable(tableName);

        if (StorageManager.ENABLE_INDEXES)
            dropTableIndexes(tableInfo);

        // Close the table.  This will purge out all dirty pages for the table
        // as well.
        closeTable(tableInfo);

        String tblFileName = getTableFileName(tableName);
        storageManager.getFileManager().deleteDBFile(tblFileName);
    }


    private void dropTableIndexes(TableInfo tableInfo) throws IOException {
        String tableName = tableInfo.getTableName();

        // We need to open the table so that we can drop all related objects,
        // such as indexes on the table, etc.
        TableSchema schema = tableInfo.getTupleFile().getSchema();

        // Check whether this table is referenced by any other tables via
        // foreign keys.  Need to check the primary key and candidate keys.

        KeyColumnRefs pk = schema.getPrimaryKey();
        if (pk != null && !pk.getReferencingIndexes().isEmpty()) {
            throw new IOException("Drop table failed due to the table" +
                " having foreign key dependencies:  " + tableName);
        }

        List<KeyColumnRefs> candKeyList = schema.getCandidateKeys();
        for (KeyColumnRefs candKey : candKeyList) {
            if (!candKey.getReferencingIndexes().isEmpty()) {
                throw new IOException("Drop table failed due to the table" +
                    " having foreign key dependencies:  " + tableName);
            }
        }

        // If we got here, the table being dropped is not a referenced table.
        // Start cleaning up various schema objects for the table.

        List<KeyColumnRefs> parentCandKeyList;
        TableInfo parentTableInfo;
        // Now drop the foreign key constraint fields that this table
        // may have on other parent tables. Scan through this table's
        // Foreign key indexes to see which tables need maintenance
        List<ForeignKeyColumnRefs> forKeyList = schema.getForeignKeys();
        for (ForeignKeyColumnRefs forKey : forKeyList) {
            // Open the parent table, and iterate through all of its primary
            // and candidate keys, dropping any foreign key constraints that
            // refer to the child table with tableName
            String parentTableName = forKey.getRefTable();
            try {
                parentTableInfo = openTable(parentTableName);
            }
            catch (Exception e) {
                throw new IOException("The referenced table, " +
                    parentTableName + ", which is referenced by " +
                    tableName + ", does not exist.");
            }

            TableSchema parentSchema = parentTableInfo.getTupleFile().getSchema();
            KeyColumnRefs parentPK = parentSchema.getPrimaryKey();

            if (parentPK != null) {
                parentPK.dropRefToTable(tableName);
            }

            parentCandKeyList = parentSchema.getCandidateKeys();
            for (KeyColumnRefs parentCandKey : parentCandKeyList) {
                parentCandKey.dropRefToTable(tableName);
            }

            // Persist the changes we made to the schema
            saveTableInfo(parentTableInfo);
        }

        // Then drop the indexes since we've checked the constraints

        IndexManager indexManager = storageManager.getIndexManager();
        for (String indexName : schema.getIndexes().keySet())
            indexManager.dropIndex(tableInfo, indexName);
    }
}
