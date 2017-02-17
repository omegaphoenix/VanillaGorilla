package edu.caltech.nanodb.indexes;


import java.io.IOException;
import java.util.List;
import java.util.Set;

import org.apache.log4j.Logger;

import edu.caltech.nanodb.commands.DeleteCommand;
import edu.caltech.nanodb.commands.ExecutionException;
import edu.caltech.nanodb.commands.UpdateCommand;

import edu.caltech.nanodb.expressions.BooleanOperator;
import edu.caltech.nanodb.expressions.ColumnValue;
import edu.caltech.nanodb.expressions.CompareOperator;
import edu.caltech.nanodb.expressions.Expression;
import edu.caltech.nanodb.expressions.LiteralValue;
import edu.caltech.nanodb.expressions.TupleLiteral;

import edu.caltech.nanodb.relations.ColumnRefs;
import edu.caltech.nanodb.relations.ConstraintViolationException;
import edu.caltech.nanodb.relations.ForeignKeyColumnRefs;
import edu.caltech.nanodb.relations.ForeignKeyValueChangeOption;
import edu.caltech.nanodb.relations.KeyColumnRefs;
import edu.caltech.nanodb.relations.Schema;
import edu.caltech.nanodb.relations.TableInfo;
import edu.caltech.nanodb.relations.TableSchema;
import edu.caltech.nanodb.relations.Tuple;

import edu.caltech.nanodb.server.EventDispatchException;
import edu.caltech.nanodb.server.NanoDBServer;
import edu.caltech.nanodb.server.RowEventListener;

import edu.caltech.nanodb.storage.TupleFile;
import edu.caltech.nanodb.storage.TableManager;
import edu.caltech.nanodb.storage.StorageManager;


/**
 * <p>
 * This class enforces all database constraints on the database schema,
 * including <tt>NOT NULL</tt> constraints, primary key/unique constraints,
 * and foreign key constraints.  It also performs the appropriate updates
 * when the target of a foreign-key reference is updated or deleted.
 * </p>
 * <p>
 * This class has some interesting design challenges to work around.  Since
 * it must sometimes update other rows in the database, the easiest way to
 * do this is to issue additional nested SQL operations against the database.
 * This also has the benefit of invoking the row-event processing code, which
 * ensures that modified tables also have constraints enforced on them.
 * </p>
 */
public class DatabaseConstraintEnforcer implements RowEventListener {
    /** A logging object for reporting anything interesting that happens. */
    private static Logger logger = Logger.getLogger(DatabaseConstraintEnforcer.class);


    private NanoDBServer server;


    private TableManager tableManager;

    private IndexManager indexManager;


    public DatabaseConstraintEnforcer(NanoDBServer server) {
        this.server = server;

        // Pull out the table manager and index manager, which are both
        // very important for this component!
        StorageManager storageManager = server.getStorageManager();
        this.tableManager = storageManager.getTableManager();
        this.indexManager = storageManager.getIndexManager();
    }


    /**
     * Perform processing before a row is inserted into a table.
     *
     * @param tableInfo the table that the tuple will be inserted into.
     * @param newTuple the new tuple that will be inserted into the table.
     */
    @Override
    public void beforeRowInserted(TableInfo tableInfo, Tuple newTuple)
            throws IOException {

        TableSchema schema = tableInfo.getTupleFile().getSchema();

        // Check NOT NULL constraints first - they are cheapest.
        checkNotNullConstraints(tableInfo, newTuple);

        // Find out all the columns that have UNIQUE constraints and find all
        // of the created indices on these columns.  Call containsTuple() with
        // the indexInfo and tuple for each of these indices.  (These indices
        // should always exist.)
        List<KeyColumnRefs> candKeyList = schema.getCandidateKeys();

        String idxName;
        IndexInfo indexInfo;

        // Check primary key
        KeyColumnRefs primaryKey = schema.getPrimaryKey();
        if (primaryKey != null) {
            idxName = primaryKey.getIndexName();
            indexInfo = indexManager.openIndex(tableInfo, idxName);

            if (containsTuple(indexInfo, newTuple)) {
                throw new EventDispatchException(
                    "Cannot add tuple due to unique constraint.");
            }
        }

        // Check candidate keys
        for (KeyColumnRefs keyColIdx : candKeyList) {
            idxName = keyColIdx.getIndexName();
            indexInfo = indexManager.openIndex(tableInfo, idxName);

            if (containsTuple(indexInfo, newTuple)) {
                throw new EventDispatchException(
                    "Cannot add tuple due to unique constraint.");
            }
        }

        // Check all the foreign keys on this table - make sure that the tuple
        // only contains values that appear in any referenced tables.
        List<ForeignKeyColumnRefs> foreignKeys = schema.getForeignKeys();
        for (ForeignKeyColumnRefs foreignKey : foreignKeys)
            checkReferencedTableForValue(tableInfo, foreignKey, newTuple);
    }


    private boolean containsTuple(IndexInfo indexInfo, Tuple tableTuple)
        throws IOException {

        Tuple searchKey = IndexUtils.makeTableSearchKey(
            indexInfo.getTableColumnRefs(), tableTuple, false);

        Tuple foundTuple = IndexUtils.findTupleInIndex(searchKey,
            indexInfo.getTupleFile());

        return (foundTuple != null);
    }


    /**
     * Perform processing after a row is inserted into a table.  For the
     * database constraint enforcer, all the work is done in the before-insert
     * handler.
     *
     * @param tblFileInfo the table that the tuple was inserted into.
     * @param newTuple    the new tuple that was inserted into the table.
     */
    @Override
    public void afterRowInserted(TableInfo tblFileInfo, Tuple newTuple)
            throws EventDispatchException {
        // Do nothing!
    }

    /**
     * Perform processing before a row is updated in a table.
     *
     * @param tableInfo the table that the tuple will be updated in.
     * @param oldTuple the old tuple in the table that is about to be updated.
     * @param newTuple the new version of the tuple.
     */
    @Override
    public void beforeRowUpdated(TableInfo tableInfo, Tuple oldTuple,
                                 Tuple newTuple) throws IOException {

        TableSchema schema = tableInfo.getSchema();

        // Check NOT NULL constraints first - they are cheapest.
        checkNotNullConstraints(tableInfo, oldTuple);

        // TODO:  Check candidate key constraints on this table.

        // Check all the foreign keys on this table - make sure that the new
        // version of the tuple only contains values that appear in any
        // referenced tables.
        List<ForeignKeyColumnRefs> foreignKeys = schema.getForeignKeys();
        for (ForeignKeyColumnRefs foreignKey : foreignKeys)
            checkReferencedTableForValue(tableInfo, foreignKey, newTuple);

        // Check if updating this tuple affects children tables via a foreign
        // key constraint.  Since the primary key is also a candidate key but
        // is stored separately, we check the PK first, and then iterate thru
        // all the candidate keys.
        try {
            KeyColumnRefs key = schema.getPrimaryKey();
            if (key != null && isTupleReferencedByFK(tableInfo, oldTuple, key))
                applyOnUpdateEffects(key, tableInfo, oldTuple, newTuple);

            List<KeyColumnRefs> candidateKeys = schema.getCandidateKeys();
            for (KeyColumnRefs candKey : candidateKeys) {
                if (isTupleReferencedByFK(tableInfo, oldTuple, candKey))
                    applyOnUpdateEffects(candKey, tableInfo, oldTuple, newTuple);
            }
        }
        catch (IOException e) {
            throw new EventDispatchException(
                    "Couldn't perform ON UPDATE operations.", e);
        }
    }

    /**
     * Perform processing after a row is updated in a table.
     *
     * @param tblFileInfo the table that the tuple was updated in.
     * @param oldValues   the old values that were in the tuple before it was
     *                    updated.
     * @param newTuple    the new tuple in the table that was updated.
     */
    @Override
    public void afterRowUpdated(TableInfo tblFileInfo, Tuple oldValues,
                                Tuple newTuple) throws EventDispatchException {
        // Do nothing!
    }

    /**
     * Perform processing after a row has been deleted from a table.
     *
     * @param tblFileInfo the table that the tuple will be deleted from.
     * @param oldTuple    the old tuple in the table that is about to be deleted.
     */
    @Override
    public void beforeRowDeleted(TableInfo tblFileInfo, Tuple oldTuple)
            throws EventDispatchException {

        // Check if deleting this tuple affects children tables via a foreign
        // key constraint.  Since the primary key is also a candidate key but
        // is stored separately, we check the PK first, and then iterate thru
        // all the candidate keys.

        TableSchema schema = tblFileInfo.getTupleFile().getSchema();

        try {
            KeyColumnRefs key = schema.getPrimaryKey();
            if (key != null && isTupleReferencedByFK(tblFileInfo, oldTuple, key))
                applyOnDeleteEffects(key, tblFileInfo, oldTuple);

            List<KeyColumnRefs> candidateKeys = schema.getCandidateKeys();
            for (KeyColumnRefs candKey : candidateKeys) {
                if (isTupleReferencedByFK(tblFileInfo, oldTuple, candKey))
                    applyOnDeleteEffects(candKey, tblFileInfo, oldTuple);
            }
        }
        catch (IOException e) {
            throw new EventDispatchException(
                "Couldn't perform ON DELETE operations.", e);
        }
    }

    /**
     * Perform processing after a row has been deleted from a table.
     *
     * @param tblFileInfo the table that the tuple was deleted from.
     * @param oldValues   the old values that were in the tuple before it was
     *                    deleted.
     */
    @Override
    public void afterRowDeleted(TableInfo tblFileInfo, Tuple oldValues)
            throws EventDispatchException {
        // Do nothing!
    }


    /**
     * This helper function verifies that a tuple being added to a table
     * satisfies all the NOT NULL constraints on the table.
     *
     * @param tableInfo the table that the tuple is being added to
     * @param tuple the tuple being added to the table
     * @throws ConstraintViolationException if the tuple has a NULL value for
     *         any not-null columns
     */
    private void checkNotNullConstraints(TableInfo tableInfo, Tuple tuple) {
        TableSchema schema = tableInfo.getSchema();
        for (int notNullCol : schema.getNotNull()) {
            logger.debug(String.format("Checking NOT NULL constraint on " +
                "column %d", notNullCol));

            if (tuple.isNullValue(notNullCol)) {
                throw new ConstraintViolationException(
                    "Cannot insert tuple " + tuple + " into table " +
                    tableInfo.getTableName() + "; NOT NULL constraint on column " +
                    schema.getColumnInfo(notNullCol).toString() +
                    " would be violated");
            }
        }
    }


    /**
     * This helper function enforces a foreign key constraint by checking the
     * referenced table to ensure that the tuple being added has values that
     * appear in the referenced table.  This constraint enforcement is
     * performed using an index on the referenced table; it is an error if
     * there is no index to perform the check.
     *
     * @param tableInfo the referencing table, to which the tuple is being
     *        added
     *
     * @param foreignKey the foreign key constraint, which specifies both the
     *        referencing table's columns and the referenced table's columns
     *
     * @param tuple the tuple being added to the referencing table
     *
     * @throws IOException if an IO error occurs during the operation
     */
    private void checkReferencedTableForValue(TableInfo tableInfo,
        ForeignKeyColumnRefs foreignKey, Tuple tuple) throws IOException {

        String tableName = tableInfo.getTableName();
        String referencedTableName = foreignKey.getRefTable();
        TableInfo referencedTableInfo;
        try {
            referencedTableInfo = tableManager.openTable(referencedTableName);
        } catch (Exception e) {
            throw new EventDispatchException("Referenced table " +
                referencedTableName + " for " + foreignKey +
                " could not be opened.", e);
        }

        TableSchema referencedSchema =
            referencedTableInfo.getTupleFile().getSchema();

        // Get the index on the referenced table's columns.
        ColumnRefs tmp = new ColumnRefs(foreignKey.getRefCols());
        KeyColumnRefs referencedKey = referencedSchema.getKeyOnColumns(tmp);
        if (referencedKey == null) {
            throw new IllegalStateException("No index on referenced table's" +
                " columns in foreign key.");
        }

        IndexInfo referencedIndexInfo = indexManager.openIndex(
            referencedTableInfo, referencedKey.getIndexName());

        TupleFile tupleFile = referencedIndexInfo.getTupleFile();

        // Create a tuple to probe the referenced table's index, so we can
        // enforce referential integrity.
        TupleLiteral probeTuple = new TupleLiteral();
        for (int i = 0; i < foreignKey.size(); i++)
            probeTuple.addValue(tuple.getColumnValue(foreignKey.getCol(i)));

        if (IndexUtils.findTupleInIndex(probeTuple, tupleFile) == null) {
            throw new ConstraintViolationException(String.format(
                "Cannot insert tuple %s into table %s; violates foreign key" +
                " constraint %s to referenced table %s", tuple, tableName,
                foreignKey.getConstraintName(), referencedTableName));
        }
    }


    /**
     * Checks if the specified tuple is referenced by tuples in any of the
     * referencing tables associated with the tuple's table.  This is done by
     * examining the tuple in the context of a candidate key {@code key}, and
     * checking every foreign-key index that references the specified
     * candidate key.
     *
     * @param tuple the tuple we are testing to see if any other tables
     *        reference the specific tuple
     *
     * @param key the candidate key that holds on the input tuple {@code tup},
     *        and that may be referenced from other tables
     *
     * @return {@code true} if the tuple is specifically referenced from one
     *         of the foreign-key tables, or {@code false} if no other tables
     *         reference this tuple
     *
     * @throws IOException if an IO error occurs while loading the referencing
     *         tables and their indexes
     */
    private boolean isTupleReferencedByFK(TableInfo tableInfo, Tuple tuple,
        KeyColumnRefs key) throws IOException {

        // Examine each foreign key that references this key.
        List<KeyColumnRefs.FKReference> fkRefs = key.getReferencingIndexes();
        for (KeyColumnRefs.FKReference fkRef : fkRefs) {
            // Open the referencing table and the index for the foreign key

            TableInfo childTblInfo = tableManager.openTable(fkRef.tableName);
            IndexInfo childIdxFileInfo =
                indexManager.openIndex(childTblInfo, fkRef.indexName);

            // Prepare to map the input tuple from the referenced schema
            // into the referencing table's schema.

            int[] idxColumns = key.getCols();
            ColumnRefs childIdx = childIdxFileInfo.getTableColumnRefs();
            int[] childIdxColumns = childIdx.getCols();

            // Initialize the probe tuple to all NULL values, then set the
            // corresponding column values from the input tuple to create
            // the value to look for.

            TupleLiteral probeTuple =
                new TupleLiteral(childTblInfo.getTupleFile().getSchema().numColumns());
            for (int i = 0; i < childIdx.size(); i++) {
                probeTuple.setColumnValue(childIdxColumns[i],
                        tuple.getColumnValue(idxColumns[i]));
            }

            // Return true if the foreign-key index has a reference to
            // this tuple.  Otherwise, go check the next index.
            if (containsTuple(childIdxFileInfo, probeTuple))
                return true;
        }

        return false;
    }


    /**
     * This function checks the <tt>ON DELETE</tt> option for each child table
     * affected by the deletion of tup due to a foreign key, and then
     * executes that option.
     */
    private void applyOnDeleteEffects(KeyColumnRefs key,
        TableInfo tableInfo, Tuple tup) throws IOException {

        String tableName = tableInfo.getTableName();
        List<KeyColumnRefs.FKReference> fkRefs = key.getReferencingIndexes();

        // Iterate through all foreign keys that reference this table.  Each
        // one may possibly require ON DELETE actions to be carried out.
        for (KeyColumnRefs.FKReference fkRef : fkRefs) {
            TableInfo childTblInfo = tableManager.openTable(fkRef.tableName);

            // We need to find the ForeignKeyColumnRefs object that
            // corresponds to the referencing index, so that we can see what
            // the referencing index's ON-DELETE action is.
            // TODO:  THIS IS HORRIBLE, provide a better way!

            ForeignKeyColumnRefs fk =
                    findReferencingTableFKIndex(childTblInfo, tableName, key);

            // Now that we have the child table's foreign key info, we can
            // figure out how to resolve the DELETE operation.

            // If delete option is RESTRICT, throw an error.
            if (fk.getOnDeleteOption() == ForeignKeyValueChangeOption.RESTRICT) {
                // Note that the index updater will have already removed the
                // tuple from the table's indexes at this point.  But, when we
                // throw an exception here, this will terminate the current
                // transaction and those index changes will be rolled back.

                throw new ConstraintViolationException("Cannot delete tuple" +
                    " on table " + tableName + " due to an ON DELETE " +
                    "RESTRICT constraint from one of its referencing tables");
            }

            String childIdxName = fkRef.indexName;

            // Create WHERE expression to find only the tuples in the child
            // WHERE foreign key columns are equal.
            Expression pred = makeReferencingTuplePredicate(tup, key,
                childTblInfo, childIdxName);

            if (fk.getOnDeleteOption() == ForeignKeyValueChangeOption.CASCADE) {
                // ON DELETE CASCADE -> drop tuples in children tables that
                // match on foreign key columns
                DeleteCommand delComm = new DeleteCommand(fkRef.tableName, pred);
                try {
                    delComm.execute(server);
                }
                catch (ExecutionException e) {
                    throw new IOException("Error trying to delete tuples " +
                            "from tables that reference table " + tableName);
                }
            }
            else if (fk.getOnDeleteOption() == ForeignKeyValueChangeOption.SET_NULL) {
                // ON DELETE SET_NULL -> set the foreign key columns in
                // the child tables to NULL where there is a match.
                UpdateCommand updateCmd = new UpdateCommand(fkRef.tableName);
                updateCmd.setWhereExpr(pred);

                // Compute the columns that need to be set to NULL.

                IndexInfo childIdxFileInfo =
                    indexManager.openIndex(childTblInfo, childIdxName);

                ColumnRefs childIdx = childIdxFileInfo.getTableColumnRefs();
                Schema childIdxSchema = childIdxFileInfo.getSchema();

                for (int i = 0; i < childIdx.size(); i++) {
                    updateCmd.addValue(
                        childIdxSchema.getColumnInfo(i).getColumnName().getColumnName(),
                        new LiteralValue(null));
                }

                try {
                    updateCmd.execute(server);
                }
                catch (ExecutionException e) {
                    throw new IOException("Error trying to update tuples " +
                            "from children tables of table " + tableName);
                }
            }
        }
    }


    /**
     * This function checks the ON UPDATE option for each child table
     * affected by the deletion of tup due to a foreign key, and then
     * executes that option.
     */
    private void applyOnUpdateEffects(KeyColumnRefs key,
        TableInfo tblFileInfo, Tuple tup, Tuple newTuple)
        throws IOException {

        String tableName = tblFileInfo.getTableName();
        List<KeyColumnRefs.FKReference> fkRefs = key.getReferencingIndexes();

        // Iterate through all foreign keys that reference this table.  Each
        // one may possibly require ON UPDATE actions to be carried out.
        for (KeyColumnRefs.FKReference fkRef : fkRefs) {
            TableInfo childTblInfo = tableManager.openTable(fkRef.tableName);

            // We need to find the ForeignKeyColumnRefs object that
            // corresponds to the referencing index, so that we can see what
            // the referencing index's ON-UPDATE action is.
            // TODO:  THIS IS HORRIBLE, provide a better way!

            ForeignKeyColumnRefs fk =
                    findReferencingTableFKIndex(childTblInfo, tableName, key);

            // Now that we have the child table's foreign key info, we can
            // figure out how to resolve the UPDATE operation.

            // If update option is RESTRICT, throw an error.
            if (fk.getOnUpdateOption() == ForeignKeyValueChangeOption.RESTRICT) {
                // Make sure we add the key back into the parent index,
                // since we arent going to be changing the table.
                // This is because we deleted the key from the index
                // in the RowEventListener before calling updateTuple().
                // TODO:  VERIFY THIS! -- Donnie
                /*
                IndexInfo parentIdxFileInfo = storageManager.openIndex(
                        tblFileInfo, key.getIndexName());
                IIndexManager parentIdxMgr = parentIdxFileInfo.getIndexManager();
                parentIdxMgr.addTuple(parentIdxFileInfo, (PageTuple) tup);
                */

                throw new IOException("Cannot update tuple on table " +
                        tableName + " due to an ON UPDATE RESTRICT " +
                        "constraint with one of its child tables");
            }

            String childIdxName = fkRef.indexName;

            IndexInfo childIdxFileInfo =
                indexManager.openIndex(childTblInfo, childIdxName);

            // This info is necessary for all the remaining ON UPDATE options.
            int[] idxColumns = key.getCols();
            ColumnRefs childIdx = childIdxFileInfo.getTableColumnRefs();
            Schema childIdxSchema = childIdxFileInfo.getSchema();

            // Create WHERE expression to find only the tuples in the child
            // WHERE foreign key columns are equal.
            Expression pred = makeReferencingTuplePredicate(tup, key,
                childTblInfo, childIdxName);

            if (fk.getOnUpdateOption() == ForeignKeyValueChangeOption.CASCADE) {
                // ON UPDATE CASCADE -> update tuples in children tables
                // that match on foreign key columns

                UpdateCommand updateCmd = new UpdateCommand(fkRef.tableName);
                updateCmd.setWhereExpr(pred);

                for (int i = 0; i < childIdx.size(); i++) {
                    updateCmd.addValue(
                        childIdxSchema.getColumnInfo(i).getColumnName().getColumnName(),
                        new LiteralValue(newTuple.getColumnValue(idxColumns[i])));
                }

                try {
                    updateCmd.execute(server);
                } catch (ExecutionException e) {
                    throw new IOException("Error trying to update tuples " +
                            "from children tables of table " + tableName);
                }
            }
            else if (fk.getOnUpdateOption() == ForeignKeyValueChangeOption.SET_NULL) {
                // ON UPDATE SET_NULL -> set the foreign key columns in the
                // child tables to NULL where there is a match.

                UpdateCommand updateCmd = new UpdateCommand(fkRef.tableName);
                updateCmd.setWhereExpr(pred);

                for (int i = 0; i < childIdx.size(); i++) {
                    updateCmd.addValue(
                        childIdxSchema.getColumnInfo(i).getColumnName().getColumnName(),
                        new LiteralValue(null));
                }

                try {
                    updateCmd.execute(server);
                }
                catch (ExecutionException e) {
                    throw new IOException("Error trying to update tuples " +
                            "from children tables of table " + tableName);
                }
                catch (Throwable t) {
                    t.printStackTrace();
                }
            }
        }
    }


    /**
     * Given a referenced table and candidate key, and a referencing table,
     * this method finds the {@link ForeignKeyColumnRefs} corresponding to
     * the reference, so that the <tt>ON UPDATE</tt> and <tt>ON DELETE</tt>
     * options can be retrieved.
     *
     * @todo (Donnie) There HAS TO be a better way to do this...
     *
     * @param referencingTblInfo
     * @param referencedTableName
     * @param referencedKey
     * @return
     */
    private ForeignKeyColumnRefs findReferencingTableFKIndex(
            TableInfo referencingTblInfo, String referencedTableName,
            KeyColumnRefs referencedKey) {

        ForeignKeyColumnRefs fk = null;

        List<ForeignKeyColumnRefs> foreignKeys =
            referencingTblInfo.getTupleFile().getSchema().getForeignKeys();

        for (ForeignKeyColumnRefs testFK : foreignKeys) {
            // Since NanoDB doesn't support multiple foreign keys
            // between two tables, we can check if we have the
            // correct foreign key by checking the names of the tables
            // TODO:  This is a bad constraint/expectation.
            if (testFK.getRefTable().equals(referencedTableName)) {
                fk = testFK;
                break;
            }
        }

        if (fk == null) {
            throw new IllegalStateException(String.format(
                    "Couldn't find the referencing foreign key from " +
                            "referencing table %s to referenced table/index %s/%s.",
                    referencingTblInfo.getTableName(), referencedTableName,
                    referencedKey.getIndexName()));
        }

        return fk;
    }


    private Expression makeReferencingTuplePredicate(Tuple referencedTuple,
        KeyColumnRefs referencedKey, TableInfo referencingTblInfo,
        String referencingIndexName) throws IOException {

        // Get the foreign key columns in child, because things are
        // going to get messy and we need to update the child.

        IndexInfo childIdxFileInfo =
            indexManager.openIndex(referencingTblInfo, referencingIndexName);

        int[] idxColumns = referencedKey.getCols();
        ColumnRefs childIdx = childIdxFileInfo.getTableColumnRefs();
        Schema childIdxSchema = childIdxFileInfo.getSchema();

        // Create WHERE expression to find only the tuples in the child
        // WHERE foreign key columns are equal.
        BooleanOperator andOp = new BooleanOperator(BooleanOperator.Type.AND_EXPR);
        for (int i = 0; i < childIdx.size(); i++) {
            CompareOperator eq = new CompareOperator(CompareOperator.Type.EQUALS,
                new ColumnValue(childIdxSchema.getColumnInfo(i).getColumnName()),
                new LiteralValue(referencedTuple.getColumnValue(idxColumns[i])));

            // If we only have one column in the foreign key, just return the
            // comparison operation, instead of wrapping it in the AND
            // expression.
            if (childIdx.size() == 1)
                return eq;

            andOp.addTerm(eq);
        }

        return andOp;
    }
}
