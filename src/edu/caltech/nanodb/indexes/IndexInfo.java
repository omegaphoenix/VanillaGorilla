package edu.caltech.nanodb.indexes;


import edu.caltech.nanodb.relations.ColumnRefs;
import edu.caltech.nanodb.relations.Schema;
import edu.caltech.nanodb.relations.TableInfo;
import edu.caltech.nanodb.relations.TableSchema;
import edu.caltech.nanodb.storage.TupleFile;


/**
 * This class is used to hold information about a single index in the
 * database.  An index is simply a tuple file built against another tuple
 * file, so this class holds information similar to a {@link TableInfo}
 * object, but with an additional specification of the mapping from the
 * table's schema to the index's schema.
 */
public class IndexInfo {

    /** The details of the table that the index is built against. */
    private TableInfo tableInfo;


    /**
     * The indexes of the columns in the table that the index is built
     * against.
     */
    private ColumnRefs indexColRefs;


    /** The tuple file that stores the index's data. */
    private TupleFile tupleFile;


    public IndexInfo(TableInfo tableInfo, ColumnRefs indexColRefs,
                     TupleFile tupleFile) {
        // tupleFile may be null!

        if (tableInfo == null)
            throw new IllegalArgumentException("tableInfo must be specified");

        if (indexColRefs == null)
            throw new IllegalArgumentException("indexColRefs must be specified");

        this.tableInfo = tableInfo;
        this.indexColRefs = indexColRefs;
        this.tupleFile = tupleFile;
    }


    /**
     * Construct an index file information object for the specified index name.
     * This constructor is used by the <tt>CREATE TABLE</tt> command to hold the
     * table's schema, before the table has actually been created.  After the
     * table is created, the {@link #setTupleFile} method is used to store the
     * database-file object onto this object.
     *
     * @param tableInfo details of the table that the index is built against
     */
    public IndexInfo(TableInfo tableInfo, ColumnRefs indexColRefs) {
        this(tableInfo, indexColRefs, null);
    }


    /**
     *
     * @param schema the table-schema object for the table that the index is
     *        defined on.
     *
     * @param indexName the unique name of the index.
     */
    private void initIndexDetails(TableSchema schema, String indexName) {
        indexColRefs = schema.getIndexes().get(indexName);
        if (indexColRefs == null) {
            throw new IllegalArgumentException("No index named " + indexName +
                " on schema " + schema);
        }

        // Get the schema of the index so that we can interpret the key-values.
//        columnInfos = schema.getColumnInfos(tableColumnIndexes.getCols());
//        columnInfos.add(new ColumnInfo("#TUPLE_FP",
//            new ColumnType(SQLDataType.FILE_POINTER)));
    }


    public TableInfo getTableInfo() {
        return tableInfo;
    }


    /**
     * Returns the tuple file that holds this index's data.
     *
     * @return the tuple file that holds this index's data.
     */
    public TupleFile getTupleFile() {
        return tupleFile;
    }


    public void setTupleFile(TupleFile tupleFile) {
        if (this.tupleFile != null) {
            throw new IllegalStateException(
                "IndexInfo already has a TupleFile object");
        }
        this.tupleFile = tupleFile;
    }


    public Schema getSchema() {
        return tupleFile.getSchema();
    }


    /**
     * Returns the index name.
     *
     * @return the index name
     */
    public String getIndexName() {
        return indexColRefs.getIndexName();
    }


    public void setIndexName(String indexName) {
        indexColRefs.setIndexName(indexName);
    }


    /**
     * Returns the name of the table that the index is built against.
     *
     * @return the name of the table that the index is built against.
     */
    public String getTableName() {
        return tableInfo.getTableName();
    }


    public ColumnRefs getTableColumnRefs() {
        return indexColRefs;
    }
}
