package edu.caltech.nanodb.storage;


import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;

import edu.caltech.nanodb.relations.ColumnRefs;
import edu.caltech.nanodb.relations.ColumnInfo;
import edu.caltech.nanodb.relations.ColumnType;
import edu.caltech.nanodb.relations.ForeignKeyColumnRefs;
import edu.caltech.nanodb.relations.ForeignKeyValueChangeOption;
import edu.caltech.nanodb.relations.KeyColumnRefs;
import edu.caltech.nanodb.relations.SQLDataType;
import edu.caltech.nanodb.relations.TableConstraintType;
import edu.caltech.nanodb.relations.TableSchema;


/**
 * <p>
 * This class contains a general purpose implementation for reading and
 * writing a table schema into a data page.  Specific file formats can
 * customize this representation by subclassing this class, and overriding
 * the various methods to modify the details they read and write.
 * </p>
 * <p>
 * The schema is written out in the following format:
 * </p>
 * <dl>
 *   <dt>Column Details:</dt>
 *   <dd>
 *     <ul>
 *       <li>Number of Columns (unsigned byte)</li>
 *       <li>For each column:
 *         <ul>
 *           <li>Column's Type (signed byte, converted to {@link SQLDataType} enum)</li>
 *           <li>If type requires a length:  Column's Max Length (unsigned short)</li>
 *           <li>Column's Name (a string up to 255 characters, stored as
 *               {@link PageReader#readVarString255}</li>
 *         </ul>
 *       </li>
 *     </ul>
 *     Column names and types are checked to be valid at load time.
 *   </dd>
 *
 *   <dt>NOT-NULL Constraints:</dt>
 *
 *   <dd>
 *     <ul>
 *       <li>Number of NOT-NULL Columns (unsigned byte)</li>
 *       <li>For each column:
 *         <ul>
 *           <li>Index of NOT-NULL Column (unsigned byte)</li>
 *         </ul>
 *       </li>
 *     </ul>
 *     Every index may only appear once.  Additionally, indexes must reference
 *     valid columns within the table schema.
 *   </dd>
 *
 *   <dt>Primary/Foreign/Candidate Key Constraints:</dt>
 *
 *   <dd>
 *     <ul>
 *       <li>Total Number of Primary + Candidate + Foreign Keys (unsigned byte)</li>
 *       <li>Primary Key (if present):
 *         <ul>
 *           <li>Constraint Type, and Name Flag (unsigned byte)</li>
 *           <li>Constraint Name (if present) (a string up to 255 characters,
 *               stored as {@link PageReader#readVarString255})</li>
 *           <li>Number of Columns in the Key (unsigned byte)</li>
 *           <li>For each column:
 *             <ul>
 *               <li>Index of Column in Key (unsigned byte)</li>
 *             </ul>
 *           </li>
 *           <li>Name of Index used to Enforce Key (a string up to 255 characters,
 *               stored as {@link PageReader#readVarString255})</li>
 *           <li>Number of Foreign-Key References to this Key (unsigned byte)</li>
 *           <li>For each FK reference:
 *             <ul>
 *               <li>Table Name (a string up to 255 characters, stored as
 *                   {@link PageReader#readVarString255})</li>
 *               <li>Index Name (a string up to 255 characters, stored as
 *                   {@link PageReader#readVarString255})</li>
 *             </ul>
 *           </li>
 *         </ul>
 *       </li>
 *       <li>For each candidate key:
 *         <ul>
 *           <li>Constraint Type, and Name Flag (unsigned byte)</li>
 *           <li>Constraint Name (if present) (a string up to 255 characters,
 *               stored as {@link PageReader#readVarString255})</li>
 *           <li>Number of Columns in the Key (unsigned byte)</li>
 *           <li>For each column:
 *             <ul>
 *               <li>Index of Column in Key (unsigned byte)</li>
 *             </ul>
 *           </li>
 *           <li>Name of Index used to Enforce Key (a string up to 255 characters,
 *               stored as {@link PageReader#readVarString255})</li>
 *           <li>Number of Foreign-Key References to this Key (unsigned byte)</li>
 *           <li>For each FK reference:
 *             <ul>
 *               <li>Table Name (a string up to 255 characters, stored as
 *                   {@link PageReader#readVarString255})</li>
 *               <li>Index Name (a string up to 255 characters, stored as
 *                   {@link PageReader#readVarString255})</li>
 *             </ul>
 *           </li>
 *         </ul>
 *       </li>
 *       <li>For each foreign key:
 *         <ul>
 *           <li>Constraint Type, and Name Flag (unsigned byte)</li>
 *           <li>Constraint Name (if present) (a string up to 255 characters,
 *               stored as {@link PageReader#readVarString255})</li>
 *           <li>Name of Referenced Table (a string up to 255 characters,
 *               stored as {@link PageReader#readVarString255})</li>
 *           <li>On-Delete Cascade Option (unsigned byte)</li>
 *           <li>On-Update Cascade Option (unsigned byte)</li>
 *           <li>Number of Columns in the Key (unsigned byte)</li>
 *           <li>For each column:
 *             <ul>
 *               <li>Index of Column in Key (unsigned byte)</li>
 *               <li>Corresponding Index of Column in Referenced Table
 *                   (unsigned byte)</li>
 *             </ul>
 *           </li>
 *         </ul>
 *       </li>
 *     </ul>
 *   </dd>
 *
 *   <dt>Indexes:</dt>
 *
 *   <dd>
 *     <ul>
 *       <li>Number of Indexes (unsigned byte)</li>
 *       <li>For each index:
 *         <ul>
 *           <li>Number of Columns in Index (unsigned byte)</li>
 *           <li>For each column:
 *             <ul>
 *               <li>Index of Column in Index (unsigned byte)</li>
 *             </ul>
 *           </li>
 *           <li>Name of Index (a string up to 255 characters,
 *               stored as {@link PageReader#readVarString255})</li>
 *         </ul>
 *       </li>
 *     </ul>
 *   </dd>
 *
 * </dl>
 */
public class SchemaWriter {
    /** A logging object for reporting anything interesting that happens. */
    private static Logger logger = Logger.getLogger(SchemaWriter.class);


    /**
     *
     * @param schema the schema to write to the page
     * @param pgWriter the {@code PageWriter} that can be used to write to the
     *        data page
     */
    public void writeTableSchema(TableSchema schema, PageWriter pgWriter) {
        // Write out the schema details now.
        logger.info("Writing table schema:  " + schema);

        int startPosition = pgWriter.getPosition();
        logger.debug("Starting at position " + startPosition);

        // Column details:
        writeColumnInfos(schema, pgWriter);

        // Write all NOT-NULL constraints:
        writeNotNullConstraints(schema, pgWriter);

        // Write all primary/candidate/foreign key constraints:
        writeKeyConstraints(schema, pgWriter);

        // Write all details of indexes on this table:
        writeIndexes(schema, pgWriter);

        // Report how much space was used by schema info.
        if (logger.isDebugEnabled()) {
            int size = pgWriter.getPosition() - startPosition;
            logger.debug("Table schema occupies " + size + " bytes.");
        }
    }


    /**
     * This helper writes out the column details of the schema.  For each
     * column, the function writes: the column's type (1 byte), plus any
     * @param schema
     * @param pgWriter
     */
    protected void writeColumnInfos(TableSchema schema, PageWriter pgWriter) {
        int startPosition = pgWriter.getPosition();

        // Record all table names that appear in the schema.  (Normally this
        // will be exactly 1, but we may want to try fun stuff with schemas
        // in the future...)

        Set<String> tableNames = schema.getTableNames();
        HashMap<String, Integer> tableNameMapping = new HashMap<String, Integer>();
        pgWriter.writeByte((byte) tableNames.size());
        logger.debug("Recording " + tableNames.size() + " table names");

        int tableIndex = 0;
        for (String tableName : tableNames) {
            pgWriter.writeVarString255(tableName);
            tableNameMapping.put(tableName, tableIndex);
            tableIndex++;
        }

        // Record information about the columns themselves.
        logger.debug("Recording " + schema.numColumns() + " columns");
        pgWriter.writeByte(schema.numColumns());
        for (ColumnInfo colInfo : schema.getColumnInfos()) {
            ColumnType colType = colInfo.getType();

            // Each column description consists of a type specification
            // (including length/precision values, if appropriate), and a
            // string specifying the column's name.

            // Write the SQL data type and any associated details.
            pgWriter.writeByte(colType.getBaseType().getTypeID());

            // If this data type requires additional details, write that as well.
            if (colType.hasLength()) {
                // CHAR and VARCHAR fields have a 2 byte length value after the type.
                pgWriter.writeShort(colType.getLength());
            }

            // Write the index of the table name for the column.
            pgWriter.writeByte(tableNameMapping.get(colInfo.getTableName()));

            // Write the column name.
            pgWriter.writeVarString255(colInfo.getName());
        }

        // Report how much space was used by column information.
        if (logger.isDebugEnabled()) {
            int size = pgWriter.getPosition() - startPosition;
            logger.debug("Column information occupies " + size + " bytes.");
        }
    }


    /**
     * @param schema
     * @param pgWriter
     */
    protected void writeNotNullConstraints(TableSchema schema,
                                           PageWriter pgWriter) {
        int startPosition = pgWriter.getPosition();

        // NOT-NULL columns are specified as a list of column-indexes in the
        // schema object.  Retrieve a list of these column indexes, and sort
        // it so that the data in the file will be easier to understand.
        ArrayList<Integer> notNullCols =
            new ArrayList<Integer>(schema.getNotNull());
        Collections.sort(notNullCols);

        int numNotNulls = notNullCols.size();
        logger.debug("Writing " + numNotNulls + " not-null column constraints.");

        pgWriter.writeByte(numNotNulls);
        for (int notNullCol : notNullCols) {
            pgWriter.writeByte(notNullCol);
        }

        if (logger.isDebugEnabled()) {
            int size = pgWriter.getPosition() - startPosition;
            logger.debug("Not-null constraints occupy " + size + " bytes.");
        }
    }


    protected void writeKeyConstraints(TableSchema schema,
                                       PageWriter pgWriter) {
        int startPosition = pgWriter.getPosition();

        // Count how many constraints we have to write.
        int numConstraints = schema.numCandidateKeys() + schema.numForeignKeys();
        KeyColumnRefs pk = schema.getPrimaryKey();
        if (pk != null)
            numConstraints++;

        logger.debug("Writing " + numConstraints + " constraints");
        pgWriter.writeByte(numConstraints);

        if (pk != null)
            writeKey(pgWriter, TableConstraintType.PRIMARY_KEY, pk);

        for (KeyColumnRefs ck : schema.getCandidateKeys())
            writeKey(pgWriter, TableConstraintType.UNIQUE, ck);

        for (ForeignKeyColumnRefs fk : schema.getForeignKeys())
            writeForeignKey(pgWriter, fk);

        if (logger.isDebugEnabled()) {
            int size = pgWriter.getPosition() - startPosition;
            logger.debug("Constraints occupy " + size + " bytes.");
        }
    }


    /**
     * This helper function writes a primary key or candidate key to the
     * schema representation.
     *
     * @param pgWriter the writer being used to write the table's schema to its
     *        header page
     *
     * @param type the constraint type, either
     *        {@link TableConstraintType#PRIMARY_KEY} or
     *        {@link TableConstraintType#FOREIGN_KEY}.
     *
     * @param key a specification of what columns appear in the key
     *
     * @throws IllegalArgumentException if the <tt>type</tt> argument is
     *         <tt>null</tt>, or is not one of the accepted values
     */
    protected void writeKey(PageWriter pgWriter, TableConstraintType type,
                            KeyColumnRefs key) {

        if (type == TableConstraintType.PRIMARY_KEY) {
            logger.debug(String.format(" * Primary key %s, enforced with " +
                                       "index %s", key, key.getIndexName()));
        }
        else if (type == TableConstraintType.UNIQUE) {
            logger.debug(String.format(" * Candidate key %s, enforced with " +
                                       "index %s", key, key.getIndexName()));
        }
        else {
            throw new IllegalArgumentException(
                "Invalid TableConstraintType value " + type);
        }

        // Constraint type, and name of constraint if it's specified.

        int typeVal = type.getTypeID();
        String cName = key.getConstraintName();
        if (cName != null)
            typeVal |= 0x80;

        pgWriter.writeByte(typeVal);
        if (cName != null)
            pgWriter.writeVarString255(cName);

        // Number of columns in the primary/candidate key
        pgWriter.writeByte(key.size());

        // The indexes of columns in the primary/candidate key
        for (int i = 0; i < key.size(); i++)
            pgWriter.writeByte(key.getCol(i));

        // Name of the index used to enforce this constraint
        pgWriter.writeVarString255(key.getIndexName());

        // Record all tables and indexes that reference this key.
        List<KeyColumnRefs.FKReference> fkRefs = key.getReferencingIndexes();
        pgWriter.writeByte(fkRefs.size());
        for (KeyColumnRefs.FKReference fkRef : fkRefs) {
            pgWriter.writeVarString255(fkRef.tableName);
            pgWriter.writeVarString255(fkRef.indexName);
        }
    }


    protected void writeForeignKey(PageWriter pgWriter,
                                   ForeignKeyColumnRefs key) {
        logger.debug(" * Foreign key " + key);

        // Constraint type, and name of constraint if it's specified.

        int type = TableConstraintType.FOREIGN_KEY.getTypeID();
        if (key.getConstraintName() != null)
            type |= 0x80;

        pgWriter.writeByte(type);
        if (key.getConstraintName() != null)
            pgWriter.writeVarString255(key.getConstraintName());

        // Name of the table that the foreign key references
        pgWriter.writeVarString255(key.getRefTable());

        // Cascade options
        pgWriter.writeByte(key.getOnDeleteOption().getTypeID());
        pgWriter.writeByte(key.getOnUpdateOption().getTypeID());

        // Number of columns in the foreign key
        pgWriter.writeByte(key.size());

        // The indexes of columns in the foreign key, and the corresponding
        // indexes of columns in the referenced table
        for (int i = 0; i < key.size(); i++) {
            pgWriter.writeByte(key.getCol(i));
            pgWriter.writeByte(key.getRefCol(i));
        }
    }


    protected void writeIndexes(TableSchema schema, PageWriter pgWriter) {
        int startPosition = pgWriter.getPosition();

        // Count how many indexes we have to write.
        int numIndexes = schema.getIndexes().size();

        logger.debug("Writing " + numIndexes + " indexes");
        pgWriter.writeByte(numIndexes);

        Map<String, ColumnRefs> idxMap = schema.getIndexes();
        for (String indexName : idxMap.keySet())
            writeIndex(pgWriter, idxMap.get(indexName));

        if (logger.isDebugEnabled()) {
            int size = pgWriter.getPosition() - startPosition;
            logger.debug("Indexes occupy " + size + " bytes in the schema");
        }
    }


    /**
     * This helper function writes an index to the table's schema stored in the
     * header page.
     *
     * @param hpWriter the writer being used to write the table's schema to its
     *        header page
     *
     * @param idx a specification of what columns appear in the index
     *
     * @throws IllegalArgumentException if the <tt>type</tt> argument is
     *         <tt>null</tt>, or is not one of the accepted values
     */
    protected void writeIndex(PageWriter hpWriter, ColumnRefs idx) {

        logger.debug(String.format(" * Index %s, enforced with index %s",
                                      idx, idx.getIndexName()));

        hpWriter.writeByte(idx.size());
        for (int i = 0; i < idx.size(); i++)
            hpWriter.writeByte(idx.getCol(i));

        // This should always be specified.
        hpWriter.writeVarString255(idx.getIndexName());
    }


    /**
     * This method opens the data file corresponding to the specified table
     * name and reads in the table's schema.
     */
    public TableSchema readTableSchema(PageReader pgReader)
        throws IOException {

        if (pgReader == null)
            throw new IllegalArgumentException("pgReader cannot be null");

        TableSchema schema = new TableSchema();

        logger.info("Reading table schema");

        int startPosition = pgReader.getPosition();
        logger.debug("Starting at position " + startPosition);

        // Column details:
        readColumnInfos(pgReader, schema);

        // Read in all NOT-NULL constraints
        readNotNullConstraints(pgReader, schema);

        // Read all details of key constraints, foreign keys, and indexes:
        readKeyConstraints(pgReader, schema);

        readIndexes(pgReader, schema);

        logger.info("Completed schema:  " + schema);
        return schema;
    }


    protected void readColumnInfos(PageReader pgReader, TableSchema schema)
        throws IOException {

        // Read in the list of table names for the schema.

        int numTables = pgReader.readUnsignedByte();
        ArrayList<String> tableNames = new ArrayList<String>(numTables);

        for (int i = 0; i < numTables; i++) {
            String tableName = pgReader.readVarString255();
            tableNames.add(tableName);
        }

        // Read in the column descriptions.

        int numCols = pgReader.readUnsignedByte();
        logger.debug("Table has " + numCols + " columns.");

        if (numCols == 0)
            throw new IOException("Table must have at least one column.");

        for (int iCol = 0; iCol < numCols; iCol++) {
            // Each column description consists of a type specification, a set
            // of flags (1 byte), and a string specifying the column's name.

            // Get the SQL data type, and begin to build the column's type
            // with that.

            byte sqlTypeID = pgReader.readByte();

            SQLDataType baseType = SQLDataType.findType(sqlTypeID);
            if (baseType == null) {
                throw new IOException("Unrecognized SQL type " + sqlTypeID +
                                      " for column " + iCol);
            }

            ColumnType colType = new ColumnType(baseType);

            // If this data type requires additional details, read that as well.
            if (colType.hasLength()) {
                // CHAR and VARCHAR fields have a 2 byte length value after
                // the type.
                colType.setLength(pgReader.readUnsignedShort());
            }

            // Read the table-name index for the column.
            int tableIndex = pgReader.readUnsignedByte();
            String tableName = tableNames.get(tableIndex);

            // Read and verify the column name.

            String colName = pgReader.readVarString255();

            if (colName.length() == 0) {
                throw new IOException("Name of column " + iCol +
                                      " is unspecified.");
            }

            for (int iCh = 0; iCh < colName.length(); iCh++) {
                char ch = colName.charAt(iCh);

                if (iCh == 0 && !(Character.isLetter(ch) || ch == '_' || ch == '#') ||
                    iCh > 0 && !(Character.isLetterOrDigit(ch) || ch == '_')) {
                    throw new IOException(String.format("Name of column " +
                        "%d \"%s\" has an invalid character at index %d.",
                        iCol, colName, iCh));
                }
            }

            ColumnInfo colInfo = new ColumnInfo(colName, tableName, colType);

            logger.debug(colInfo);

            schema.addColumnInfo(colInfo);
        }
    }


    protected void readNotNullConstraints(PageReader pgReader,
                                          TableSchema schema) {
        int numNotNulls = pgReader.readUnsignedByte();
        logger.debug("Reading " + numNotNulls + " not-null constraints");

        for (int i = 0; i < numNotNulls; i++) {
            int index = pgReader.readUnsignedByte();
            if (!schema.addNotNull(index)) {
                throw new IllegalStateException("Index " + index +
                    " appears multiple times in NOT-NULL specification!");
            }
        }
    }


    protected void readKeyConstraints(PageReader pgReader, TableSchema schema)
        throws IOException {

        int numConstraints = pgReader.readUnsignedByte();
        logger.debug("Reading " + numConstraints + " constraints");

        for (int i = 0; i < numConstraints; i++) {
            int cTypeID = pgReader.readUnsignedByte();
            TableConstraintType cType =
                TableConstraintType.findType((byte) (cTypeID & 0x7F));
            if (cType == null)
                throw new IOException("Unrecognized constraint-type value " + cTypeID);

            KeyColumnRefs key;
            switch (cType) {
                case PRIMARY_KEY:
                    key = readKey(pgReader, cTypeID,
                                  TableConstraintType.PRIMARY_KEY);
                    schema.setPrimaryKey(key);
                    break;

                case UNIQUE:
                    key = readKey(pgReader, cTypeID,
                                  TableConstraintType.UNIQUE);
                    schema.addCandidateKey(key);
                    break;

                case FOREIGN_KEY:
                    ForeignKeyColumnRefs fkey =
                        readForeignKey(pgReader, cTypeID);
                    schema.addForeignKey(fkey);
                    break;

                default:
                    throw new IOException(
                        "Encountered unrecognized constraint type " + cType);
            }
        }
    }


    /**
     * This helper function writes a primary key or candidate key to the table's
     * schema stored in the header page.
     *
     * @param pgReader the writer being used to write the table's schema to its
     *        header page
     *
     * @param typeID the unsigned-byte value read from the table's header page,
     *        corresponding to this key's type.  Although this value is already
     *        parsed before calling this method, it also contains flags that
     *        this method handles, so it must be passed in as well.
     *
     * @param type the constraint type, either
     *        {@link TableConstraintType#PRIMARY_KEY} or
     *        {@link TableConstraintType#FOREIGN_KEY}.
     *
     * @return a specification of the key, including its name, what columns
     *         appear in the key, and what index is used to enforce the key
     *
     * @throws IllegalArgumentException if the <tt>type</tt> argument is
     *         <tt>null</tt>, or is not one of the accepted values
     */
    protected KeyColumnRefs readKey(PageReader pgReader, int typeID,
                                       TableConstraintType type) {

        if (type == TableConstraintType.PRIMARY_KEY) {
            logger.debug(" * Reading primary key");
        }
        else if (type == TableConstraintType.UNIQUE) {
            logger.debug(" * Reading candidate key");
        }
        else {
            throw new IllegalArgumentException(
                "Unhandled TableConstraintType value " + type);
        }

        // The constraint may or may not be named; if so, read in the name.
        String constraintName = null;
        if ((typeID & 0x80) != 0)
            constraintName = pgReader.readVarString255();

        // The indexes of the columns in the key.
        int keySize = pgReader.readUnsignedByte();
        int[] keyCols = new int[keySize];
        for (int i = 0; i < keySize; i++)
            keyCols[i] = pgReader.readUnsignedByte();

        // This should always be specified.
        String indexName = pgReader.readVarString255();

        KeyColumnRefs key = new KeyColumnRefs(indexName, keyCols, type);
        key.setConstraintName(constraintName);

        // Read in all tables and indexes that reference this key.
        int refSize = pgReader.readUnsignedByte();
        for (int i = 0; i < refSize; i++) {
            // First string value is table name, second one is index name.
            key.addRef(pgReader.readVarString255(),
                       pgReader.readVarString255());
        }
        return key;
    }


    protected ForeignKeyColumnRefs readForeignKey(PageReader pgReader, int typeID) {
        logger.debug(" * Reading foreign key");

        // Read the constraint's name, if it's specified.

        String constraintName = null;
        if ((typeID & 0x80) != 0)
            constraintName = pgReader.readVarString255();

        // Name of the table that the foreign key references
        String refTableName = pgReader.readVarString255();

        // Cascade options.
        // TODO:  Verify that the options are valid.  We kinda assume it here.
        ForeignKeyValueChangeOption onDeleteOption =
            ForeignKeyValueChangeOption.findType(pgReader.readUnsignedByte());
        ForeignKeyValueChangeOption onUpdateOption =
            ForeignKeyValueChangeOption.findType(pgReader.readUnsignedByte());
        int keySize = pgReader.readUnsignedByte();

        // The indexes of columns in the foreign key, and the corresponding
        // indexes of columns in the referenced table
        int[] keyCols = new int[keySize];
        int[] refCols = new int[keySize];
        for (int i = 0; i < keySize; i++) {
            keyCols[i] = pgReader.readUnsignedByte();
            refCols[i] = pgReader.readUnsignedByte();
        }

        ForeignKeyColumnRefs fk = new ForeignKeyColumnRefs(
            keyCols, refTableName, refCols, onDeleteOption, onUpdateOption);
        fk.setConstraintName(constraintName);

        return fk;
    }


    protected void readIndexes(PageReader pgReader, TableSchema schema) {
        int numIndexes = pgReader.readUnsignedByte();
        logger.debug("Reading " + numIndexes + " indexes");
        for (int i = 0; i < numIndexes; i++)
            schema.addIndex(readIndex(pgReader));
    }


    /**
     * This helper function reads an index to the table's schema stored
     * in the header page.
     *
     * @param pgReader the reader being used to read the table's schema to its
     *        header page
     *
     * @return a specification of the index, including its name, what columns
     *         appear in the index
     *
     * @throws IllegalArgumentException if the <tt>type</tt> argument is
     *         <tt>null</tt>, or is not one of the accepted values
     */
    protected ColumnRefs readIndex(PageReader pgReader) {

        logger.debug(" * Reading index");

        int idxSize = pgReader.readUnsignedByte();
        int[] idxCols = new int[idxSize];
        for (int i = 0; i < idxSize; i++)
            idxCols[i] = pgReader.readUnsignedByte();

        // This should always be specified.
        String indexName = pgReader.readVarString255();

        return new ColumnRefs(indexName, idxCols);
    }
}
