package edu.caltech.nanodb.commands;


import java.io.FileNotFoundException;
import java.io.IOException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

import org.apache.log4j.Logger;

import edu.caltech.nanodb.indexes.IndexManager;
import edu.caltech.nanodb.indexes.IndexUtils;

import edu.caltech.nanodb.relations.ColumnInfo;
import edu.caltech.nanodb.relations.ColumnRefs;
import edu.caltech.nanodb.relations.ForeignKeyColumnRefs;
import edu.caltech.nanodb.relations.KeyColumnRefs;
import edu.caltech.nanodb.relations.TableConstraintType;
import edu.caltech.nanodb.relations.TableInfo;
import edu.caltech.nanodb.relations.TableSchema;

import edu.caltech.nanodb.server.NanoDBServer;
import edu.caltech.nanodb.storage.TableManager;
import edu.caltech.nanodb.storage.StorageManager;


/**
 * This command handles the <tt>CREATE TABLE</tt> DDL operation.
 */
public class CreateTableCommand extends Command {

    /** A logging object for reporting anything interesting that happens. */
    private static Logger logger = Logger.getLogger(CreateTableCommand.class);


    public static final String PROP_PAGESIZE = "pagesize";


    public static final String PROP_STORAGE = "storage";


    /** Name of the table to be created. */
    private String tableName;


    /** If this flag is {@code true} then the table is a temporary table. */
    private boolean temporary;


    /**
     * If this flag is {@code true} then the create-table operation should
     * only be performed if the specified table doesn't already exist.
     */
    private boolean ifNotExists;


    /** List of column-declarations for the new table. */
    private List<ColumnInfo> columnInfos = new ArrayList<>();


    /** List of constraints for the new table. */
    private List<ConstraintDecl> constraints = new ArrayList<>();


    /** Any additional properties specified in the command. */
    private CommandProperties properties;


    /**
     * Create a new object representing a <tt>CREATE TABLE</tt> statement.
     *
     * @param tableName the name of the table to be created
     * @param temporary true if the table is a temporary table, false otherwise
     * @param ifNotExists If this flag is true, the table will only be created
     *        if it doesn't already exist.
     */
    public CreateTableCommand(String tableName,
                              boolean temporary, boolean ifNotExists) {
        super(Command.Type.DDL);

        if (tableName == null)
            throw new IllegalArgumentException("tableName cannot be null");

        this.tableName = tableName;
        this.temporary = temporary;
        this.ifNotExists = ifNotExists;
    }


    public void setProperties(CommandProperties properties) {
        this.properties = properties;
    }


    public CommandProperties getProperties() {
        return properties;
    }


    /**
     * Adds a column description to this create-table command.  This method is
     * primarily used by the SQL parser.
     *
     * @param colInfo the details of the column to add
     *
     * @throws NullPointerException if colDecl is null
     */
    public void addColumn(ColumnInfo colInfo) {
        if (colInfo == null)
            throw new IllegalArgumentException("colInfo cannot be null");

        if (!tableName.equals(colInfo.getTableName())) {
            colInfo = new ColumnInfo(colInfo.getName(), tableName,
                colInfo.getType());
        }

        columnInfos.add(colInfo);
    }


    /**
     * Adds a constraint to this create-table command.  This method is primarily
     * used by the SQL parser.
     *
     * @param con the details of the table constraint to add
     *
     * @throws NullPointerException if con is null
     */
    public void addConstraint(ConstraintDecl con) {
        if (con == null)
            throw new IllegalArgumentException("con cannot be null");

        constraints.add(con);
    }


    @Override
    public void execute(NanoDBServer server) throws ExecutionException {

        StorageManager storageManager = server.getStorageManager();
        TableManager tableManager = storageManager.getTableManager();

        // See if the table already exists.
        if (ifNotExists) {
            try {
                if (tableManager.tableExists(tableName)) {
                    out.printf("Table %s already exists; skipping create-table.%n",
                        tableName);
                    return;
                }
            }
            catch (IOException e) {
                // Some other unexpected exception occurred.  Report an error.
                throw new ExecutionException(
                    "Exception while trying to determine if table " +
                    tableName + " exists.", e);
            }
        }

        // Set up the table's schema based on the command details.

        logger.debug("Creating a TableSchema object for the new table " +
            tableName + ".");

        TableSchema schema = new TableSchema();
        for (ColumnInfo colInfo : columnInfos) {
            try {
                schema.addColumnInfo(colInfo);
            }
            catch (IllegalArgumentException iae) {
                throw new ExecutionException("Duplicate or invalid column \"" +
                    colInfo.getName() + "\".", iae);
            }
        }

        // Do some basic verification of the table constraints:
        //  * Verify that all named constraints are uniquely named.
        //  * Open all tables referenced by foreign-key constraints, to ensure
        //    they exist.  (More verification will occur later.)
        HashSet<String> constraintNames = new HashSet<>();
        HashMap<String, TableInfo> referencedTables = new HashMap<>();
        for (ConstraintDecl cd: constraints) {
            String name = cd.getName();
            if (name != null && !constraintNames.add(name)) {
                throw new ExecutionException("Constraint name " + name +
                    " appears multiple times.");
            }

            if (cd.getType() == TableConstraintType.FOREIGN_KEY) {
                String refTableName = cd.getRefTable();
                try {
                    TableInfo refTblInfo = tableManager.openTable(refTableName);
                    referencedTables.put(refTableName, refTblInfo);
                }
                catch (FileNotFoundException e) {
                    throw new ExecutionException(String.format(
                        "Referenced table %s doesn't exist.", refTableName), e);
                }
                catch (IOException e) {
                    throw new ExecutionException(String.format(
                        "Error while loading schema for referenced table %s.",
                        refTableName), e);
                }
            }
        }

        // Get the table manager and create the table.

        logger.debug("Creating the new table " + tableName + " on disk.");
        TableInfo tblInfo;
        try {
            tblInfo = tableManager.createTable(tableName, schema, properties);
        }
        catch (IOException ioe) {
            throw new ExecutionException("Could not create table \"" +
                tableName + "\".  See nested exception for details.", ioe);
        }
        logger.debug("New table " + tableName + " was created.");

        // Now, initialize all the constraints on the table.

        try {
            initTableConstraints(storageManager, tblInfo, referencedTables);
        }
        catch (IOException e) {
            throw new ExecutionException(
                "Couldn't initialize all constraints on table " + tableName, e);
        }

        out.println("Created table:  " + tableName);
    }


    private void initTableConstraints(StorageManager storageManager,
        TableInfo tableInfo, HashMap<String, TableInfo> referencedTables)
        throws ExecutionException, IOException {

        if (constraints.isEmpty()) {
            logger.debug("No table constraints specified, our work is done.");
            return;
        }

        TableManager tableManager = storageManager.getTableManager();
        IndexManager indexManager = storageManager.getIndexManager();
        TableSchema tableSchema = tableInfo.getSchema();

        logger.debug("Adding " + constraints.size() +
            " constraints to the table.");

        HashSet<String> constraintNames = new HashSet<>();

        for (ConstraintDecl cd : constraints) {
            // Make sure that if constraint names are specified, every
            // constraint is actually uniquely named.
            if (cd.getName() != null) {
                if (!constraintNames.add(cd.getName())) {
                    throw new ExecutionException("Constraint name " +
                        cd.getName() + " appears multiple times.");
                }
            }

            TableConstraintType type = cd.getType();
            if (type == TableConstraintType.PRIMARY_KEY) {
                // Make a primary key constraint and put it on the schema.

                int[] cols = tableSchema.getColumnIndexes(cd.getColumnNames());
                KeyColumnRefs pk = new KeyColumnRefs(cd.getName(), cols,
                    TableConstraintType.PRIMARY_KEY);

                // Make the index.  This also updates the table schema with
                // the fact that there is another candidate key on the table.
                indexManager.addIndexToTable(tableInfo, pk);

                // Add NOT NULL constraints for all primary key columns.
                for (int i = 0; i < pk.size(); i++)
                    tableSchema.addNotNull(pk.getCol(i));

                tableSchema.setPrimaryKey(pk);
            }
            else if (type == TableConstraintType.UNIQUE) {
                // Make a unique key constraint and put it on the schema.

                int[] cols = tableSchema.getColumnIndexes(cd.getColumnNames());
                KeyColumnRefs ck = new KeyColumnRefs(cd.getName(), cols,
                    TableConstraintType.UNIQUE);

                // Make the index.  This also updates the table schema with
                // the fact that there is another candidate key on the table.
                indexManager.addIndexToTable(tableInfo, ck);
            }
            else if (type == TableConstraintType.FOREIGN_KEY) {
                // Make a foreign key constraint and put it on the schema.
                // This involves three steps:
                // 1)  Create the foreign-key constraint on this table's schema
                // 2)  If there isn't already an index on the referencing
                //     columns, create a non-unique index.
                // 3)  Update the referenced table's schema to record the
                //     foreign-key reference from this table.

                // This should never be null since we already resolved all
                // foreign-key table references earlier.
                TableInfo refTableInfo = referencedTables.get(cd.getRefTable());
                TableSchema refTableSchema = refTableInfo.getSchema();

                // The makeForeignKey() method ensures that the referenced
                // columns are also a candidate key (or primary key) on the
                // referenced table.
                ForeignKeyColumnRefs fk = IndexUtils.makeForeignKey(
                    tableSchema, cd.getColumnNames(),
                    cd.getRefTable(), refTableSchema, cd.getRefColumnNames(),
                    cd.getOnDeleteOption(), cd.getOnUpdateOption());

                fk.setConstraintName(cd.getName());
                tableSchema.addForeignKey(fk);

                // Check if there is already an index on the foreign-key
                // columns.  If there is not, we will create a non-unique
                // index on those columns.

                int[] fkCols = tableSchema.getColumnIndexes(cd.getColumnNames());
                ColumnRefs fkColRefs = tableSchema.getKeyOnColumns(new ColumnRefs(fkCols));
                if (fkColRefs == null) {
                    // Need to make a new index for this foreign-key reference
                    fkColRefs = new ColumnRefs(cd.getName(), fkCols);
                    fkColRefs.setConstraintType(TableConstraintType.FOREIGN_KEY);

                    // Make the index.
                    indexManager.addIndexToTable(tableInfo, fkColRefs);

                    logger.debug(String.format(
                        "Created index %s on table %s to enforce foreign key.",
                        fkColRefs.getIndexName(), tableInfo.getTableName()));
                }

                // Finally, update the referenced table's schema to record
                // that there is a foreign-key reference to the table.
                refTableSchema.addRefTable(tableName, fkColRefs.getIndexName(),
                    fk.getRefCols());
                tableManager.saveTableInfo(refTableInfo);
            }
            else if (type == TableConstraintType.NOT_NULL) {
                int idx = tableSchema.getColumnIndex(cd.getColumnNames().get(0));
                tableSchema.addNotNull(idx);
            }
            else {
                throw new ExecutionException("Unexpected constraint type " +
                    cd.getType());
            }
        }
    }


    @Override
    public String toString() {
        return "CreateTable[" + tableName + "]";
    }


    /**
     * Returns a verbose, multi-line string containing all of the details of
     * this table.
     *
     * @return a detailed description of the table described by this command
     */
    public String toVerboseString() {
        StringBuilder strBuf = new StringBuilder();

        strBuf.append(toString());
        strBuf.append('\n');

        for (ColumnInfo colInfo : columnInfos) {
            strBuf.append('\t');
            strBuf.append(colInfo.toString());
            strBuf.append('\n');
        }

        for (ConstraintDecl con : constraints) {
            strBuf.append('\t');
            strBuf.append(con.toString());
            strBuf.append('\n');
        }

        return strBuf.toString();
    }
}
