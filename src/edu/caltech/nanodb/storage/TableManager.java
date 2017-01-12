package edu.caltech.nanodb.storage;


import java.io.IOException;
import java.util.ArrayList;

import edu.caltech.nanodb.commands.CommandProperties;
import edu.caltech.nanodb.relations.TableInfo;
import edu.caltech.nanodb.relations.TableSchema;


/**
 * This interface specifies the operations performed specifically on table
 * files.
 */
public interface TableManager {

    /**
     * Returns an ArrayList of the names of the tables available.
     * @return a list of table names
     */
    public ArrayList<String> getTables();

    /**
     * Returns true if the specified table exists, or false otherwise.
     *
     * @param tableName the name of the table to check for existence
     *
     * @return {@code true} if the specified table exists, or {@code false}
     *         if it doesn't exist
     *
     * @throws IOException if an IO error occurs while determining if the
     *         table exists
     */
    boolean tableExists(String tableName) throws IOException;

    /**
     * Creates a new table file with the table-name and schema specified in
     * the passed-in {@link TableInfo} object.  Additional details such as
     * the data file and the table manager are stored into the passed-in
     * {@code TableInfo} object upon successful creation of the new table.
     *
     * @throws IOException if the file cannot be created, or if an error
     *         occurs while storing the initial table data.
     */
    TableInfo createTable(String tableName, TableSchema schema,
        CommandProperties properties) throws IOException;


    /**
     * This method opens the data file corresponding to the specified table
     * name and reads in the table's schema.  If the table is already open
     * then the cached data is simply returned.
     *
     * @param tableName the name of the table to open.  This is generally
     *        whatever was specified in a SQL statement that references the
     *        table.
     *
     * @return an object that holds the details of the open table
     *
     * @throws java.io.FileNotFoundException if no table-file exists for the
     *         table; in other words, it doesn't yet exist.
     *
     * @throws IOException if an IO error occurs when attempting to open the
     *         table.
     */
    TableInfo openTable(String tableName) throws IOException;


    /**
     * This method saves the schema and other details of a table into the
     * backing table file, using the schema and other details specified in
     * the passed-in {@link TableInfo} object.  It is used to initialize
     * new tables, and also to update tables when their schema changes.
     *
     * @param tableInfo This object is an in/out parameter.  It is used to
     *        specify the name and schema of the new table being created.
     *        When the table is successfully created, the object is updated
     *        with the actual file that the table's schema and data are
     *        stored in.
     *
     * @throws IOException if the file cannot be created, or if an error
     *         occurs while storing the initial table data.
     */
    void saveTableInfo(TableInfo tableInfo) throws IOException;


    /**
     * This function analyzes the specified table, and updates the table's
     * statistics to be the most up-to-date values.
     *
     * @param tableInfo the opened table to analyze.
     *
     * @throws IOException if an IO error occurs while trying to analyze the
     *         table.
     */
    void analyzeTable(TableInfo tableInfo) throws IOException;


    /**
     * This method closes a table file that is currently open, flushing any
     * dirty pages to the table's storage in the process.
     *
     * @param tableInfo the table to close
     *
     * @throws IOException if an IO error occurs while attempting to close the
     *         table.  This could occur, for example, if dirty pages are being
     *         flushed to disk and a write error occurs.
     */
    void closeTable(TableInfo tableInfo) throws IOException;


    /**
     * Drops the specified table from the database.
     *
     * @param tableName the name of the table to drop
     *
     * @throws IOException if an IO error occurs while trying to delete the
     *         table's backing storage.
     */
    void dropTable(String tableName) throws IOException;
}
