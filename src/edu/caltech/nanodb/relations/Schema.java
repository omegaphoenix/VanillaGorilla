package edu.caltech.nanodb.relations;


import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

import edu.caltech.nanodb.expressions.ColumnName;


/**
 * <p>
 * A schema is an ordered collection of column names and associated types.
 * </p>
 * <p>
 * Many different entities in the database code can have schema associated
 * with them.  Both tables and tuples have schemas, for obvious reasons.
 * <tt>SELECT</tt> and <tt>FROM</tt> clauses also have schemas, used by the
 * database engine to verify the semantics of database queries.  Finally,
 * relational algebra plan nodes also have schemas, which specify the kinds of
 * tuples that they generate.
 * </p>
 */
public class Schema implements Serializable, Iterable<ColumnInfo> {

    /**
     * This helper class is used for the internal hashed column structure, so
     * that we can do fast lookups based on table names and column names.
     */
    private static class IndexedColumnInfo implements Serializable {
        /** The index in the schema that the column appears at. */
        public int colIndex;

        /** The details of the column at the stored index. */
        public ColumnInfo colInfo;

        /** Stores the specified index and column-info value. */
        public IndexedColumnInfo(int colIndex, ColumnInfo colInfo) {
            if (colInfo == null)
                throw new NullPointerException("colInfo cannot be null");

            if (colIndex < 0) {
                throw new IllegalArgumentException("colIndex must be >= 0; got " +
                    colIndex);
            }

            this.colIndex = colIndex;
            this.colInfo = colInfo;
        }
    }


    /**
     * The collection of the column-info objects describing the columns in the
     * schema.
     */
    private ArrayList<ColumnInfo> columnInfos;


    /**
     * A mapping that provides fast lookups for columns based on table name and
     * column name.  The outer hash-map has table names for keys; "no table" is
     * indicated with a <code>null</code> key, which {@link java.util.HashMap}
     * supports.  The inner hash-map has column names for keys, and maps to
     * column information objects.
     */
    private HashMap<String, HashMap<String, IndexedColumnInfo>> colsHashedByTable;


    /**
     * A mapping that provides fast lookups for columns based only on column
     * name.  Because multiple columns could have the same column name (but
     * different table names) in a single schema, the values in the mapping are
     * lists.
     */
    private HashMap<String, ArrayList<IndexedColumnInfo>> colsHashedByColumn;



    public Schema() {
        columnInfos = new ArrayList<ColumnInfo>();
        colsHashedByTable =
            new HashMap<String, HashMap<String, IndexedColumnInfo> >();
        colsHashedByColumn = new HashMap<String, ArrayList<IndexedColumnInfo>>();
    }


    public Schema(List<ColumnInfo> colInfos) throws SchemaNameException {
        this();
        append(colInfos);
    }


    /**
     * Construct a copy of the specified schema object.
     *
     * @param s the schema object to copy
     */
    public Schema(Schema s) {
        this();
        append(s);
    }


    /**
     * Returns the number of columns in the schema.
     *
     * @return the number of columns in the schema.
     */
    public int numColumns() {
        return columnInfos.size();
    }


    /**
     * Returns the <tt>ColumnInfo</tt> object describing the column at the
     * specified index.  Column indexes are numbered from 0.
     *
     * @param i the index to retrieve the column-info for
     *
     * @return the <tt>ColumnInfo</tt> object describing the name and type of
     *         the column
     */
    public ColumnInfo getColumnInfo(int i) {
        return columnInfos.get(i);
    }


    public List<ColumnInfo> getColumnInfos() {
        return Collections.unmodifiableList(columnInfos);
    }


    public ArrayList<ColumnInfo> getColumnInfos(int[] colIndexes) {
        ArrayList<ColumnInfo> result = new ArrayList<ColumnInfo>(colIndexes.length);

        for (int i = 0; i < colIndexes.length; i++)
            result.add(getColumnInfo(colIndexes[i]));

        return result;
    }


    public Iterator<ColumnInfo> iterator() {
        return Collections.unmodifiableList(columnInfos).iterator();
    }


    public int addColumnInfo(ColumnInfo colInfo) {
        if (colInfo == null)
            throw new NullPointerException("colInfo cannot be null");

        String colName = colInfo.getName();
        String tblName = colInfo.getTableName();

        // Check the hashed-columns structure to see if this column already
        // appears in the schema.

        HashMap<String, IndexedColumnInfo> colMap = colsHashedByTable.get(tblName);
        if (colMap != null && colMap.containsKey(colName)) {
            throw new SchemaNameException("Specified column " + colInfo +
            " is a duplicate of an existing column.");
        }

        int colIndex = columnInfos.size();
        columnInfos.add(colInfo);

        IndexedColumnInfo indexedColInfo = new IndexedColumnInfo(colIndex, colInfo);

        // Update the hashed-columns structures for fast/easy lookup.

        if (colMap == null) {
            colMap = new HashMap<String, IndexedColumnInfo>();
            colsHashedByTable.put(tblName, colMap);
        }
        colMap.put(colName, indexedColInfo);

        ArrayList<IndexedColumnInfo> colList = colsHashedByColumn.get(colName);
        if (colList == null) {
            colList = new ArrayList<IndexedColumnInfo>();
            colsHashedByColumn.put(colName, colList);
        }
        colList.add(indexedColInfo);

        // Finally, return the index.

        return colIndex;
    }


    /**
     * Append another schema to this schema.
     *
     * @throws SchemaNameException if any of the input column-info objects
     *         overlap the names of columns already in the schema.
     */
    public void append(Schema s) throws SchemaNameException {
        for (ColumnInfo colInfo : s)
             addColumnInfo(colInfo);
    }


    /**
     * Append a list of column-info objects to this schema.
     *
     * @throws SchemaNameException if multiple of the input column-info objects
     *         have duplicate column names, or overlap the names of columns
     *         already in the schema.
     */
    public void append(Collection<ColumnInfo> colInfos) throws SchemaNameException {

        for (ColumnInfo colInfo : colInfos)
            addColumnInfo(colInfo);
    }


    /**
     * Returns a set containing all table names that appear in this schema.
     * Note that this may include {@code null} if there are columns with no
     * table name specified!
     */
    public Set<String> getTableNames() {
        return Collections.unmodifiableSet(colsHashedByTable.keySet());
    }


    /**
     * This helper method returns the names of all tables that appear in both
     * this schema and the specified schema.  Note that not all columns of a
     * given table must be present for the table to be included in the result;
     * there just has to be at least one column from the table in both schemas
     * for it to be included in the result.
     *
     * @param s the other schema to compare this schema to
     * @return a set containing the names of all tables that appear in both
     *         schemas
     */
    public Set<String> getCommonTableNames(Schema s) {
        HashSet<String> shared = new HashSet<String>(colsHashedByTable.keySet());
        shared.retainAll(s.getTableNames());
        return shared;
    }


    /**
     * Returns a set containing all column names that appear in this schema.
     * Note that a column-name may be used by multiple columns, if it is
     * associated with multiple table names in this schema.
     *
     * @return a set containing all column names that appear in this schema.
     */
    public Set<String> getColumnNames() {
        return Collections.unmodifiableSet(colsHashedByColumn.keySet());
    }


    /**
     * Returns the names of columns that are common between this schema and the
     * specified schema.  This kind of operation is mainly used for resolving
     * <tt>NATURAL</tt> joins.
     *
     * @param s the schema to compare to this schema
     * @return a set of the common column names
     */
    public Set<String> getCommonColumnNames(Schema s) {
        HashSet<String> shared = new HashSet<String>(colsHashedByColumn.keySet());
        shared.retainAll(s.getColumnNames());
        return shared;
    }


    /**
     * Returns the number of columns that have the specified column name.  Note
     * that multiple columns can have the same column name but different table
     * names.
     *
     * @param colName the column name to return the count for
     * @return the number of columns with the specified column name
     */
    public int numColumnsWithName(String colName) {
        ArrayList<IndexedColumnInfo> list = colsHashedByColumn.get(colName);
        if (list != null)
            return list.size();

        return 0;
    }


    /**
     * This helper method returns true if this schema contains any columns with
     * the same column name but different table names.  If so, the schema is not
     * valid for use on one side of a <tt>NATURAL</tt> join.
     *
     * @return true if the schema has multiple columns with the same column name
     *         but different table names, or false otherwise.
     */
    public boolean hasMultipleColumnsWithSameName() {
        for (String cName : colsHashedByColumn.keySet()) {
            if (colsHashedByColumn.get(cName).size() > 1)
                return true;
        }
        return false;
    }


    /**
     * Presuming that exactly one column has the specified name, this method
     * returns the column information for that column name.
     *
     * @param colName the name of the column to retrieve the information for
     *
     * @return the column information for the specified column
     *
     * @throws SchemaNameException if the specified column name doesn't appear
     *         in this schema, or if it appears multiple times
     */
    public ColumnInfo getColumnInfo(String colName) {
        ArrayList<IndexedColumnInfo> list = colsHashedByColumn.get(colName);
        if (list == null || list.size() == 0)
            throw new SchemaNameException("No columns with name " + colName);

        if (list.size() > 1)
            throw new SchemaNameException("Multiple columns with name " + colName);

        return list.get(0).colInfo;
    }


    /**
     * This method iterates through all columns in this schema and sets them all
     * to be on the specified table.  This method will throw an exception if the
     * result would be an invalid schema with duplicate column names.
     *
     * @throws SchemaNameException if the schema contains columns with the same
     *         column name but different table names.  In this case, resetting the
     *         table name will produce an invalid schema with ambiguous column
     *         names.
     *
     * @design (donnie) At present, this method does this by replacing each
     *         {@link edu.caltech.nanodb.relations.ColumnInfo} object with a new
     *         object with updated information.  This is because
     *         <code>ColumnInfo</code> is currently immutable.
     */
    public void setTableName(String tableName) throws SchemaNameException {
        // First, verify that overriding the table names will not produce multiple
        // ambiguous column names.
        ArrayList<String> duplicateNames = null;

        for (Map.Entry<String, ArrayList<IndexedColumnInfo> > entry :
             colsHashedByColumn.entrySet()) {

            if (entry.getValue().size() > 1) {
                if (duplicateNames == null)
                    duplicateNames = new ArrayList<String>();

                duplicateNames.add(entry.getKey());
            }
        }

        if (duplicateNames != null) {
            throw new SchemaNameException("Overriding table-name to \"" +
                tableName + "\" would produce ambiguous columns:  " +
                duplicateNames);
        }

        // If we get here, we know that we can safely override the table name for
        // all columns.

        ArrayList<ColumnInfo> oldColInfos = columnInfos;

        columnInfos = new ArrayList<ColumnInfo>();
        colsHashedByTable =
            new HashMap<String, HashMap<String, IndexedColumnInfo>>();
        colsHashedByColumn = new HashMap<String, ArrayList<IndexedColumnInfo>>();

        // Iterate over the columns in the same order as they were in originally.
        // For each one, override the table name, then use addColumnInfo() to
        // properly update the internal hash structure.

        for (ColumnInfo colInfo : oldColInfos) {
            ColumnInfo newColInfo =
                new ColumnInfo(colInfo.getName(), tableName, colInfo.getType());

            addColumnInfo(newColInfo);
        }
    }


    public int getColumnIndex(ColumnName colName) {
        if (colName.isColumnWildcard())
            throw new IllegalArgumentException("colName cannot be a wildcard");

        return getColumnIndex(colName.getTableName(), colName.getColumnName());
    }


    public int getColumnIndex(ColumnInfo colInfo) {
        return getColumnIndex(colInfo.getTableName(), colInfo.getName());
    }


    public int getColumnIndex(String colName) {
        return getColumnIndex(null, colName);
    }


    public int getColumnIndex(String tblName, String colName) {
        ArrayList<IndexedColumnInfo> colList = colsHashedByColumn.get(colName);

        if (colList == null)
            return -1;

        if (tblName == null) {
            if (colList.size() > 1) {
                throw new SchemaNameException("Column name \"" + colName +
                    "\" is ambiguous in this schema.");
            }

            return colList.get(0).colIndex;
        }
        else {
            // Table-name is specified.

            for (IndexedColumnInfo c : colList) {
                if (tblName.equals(c.colInfo.getTableName()))
                    return c.colIndex;
            }
        }

        return -1;
    }


    /**
     * Given a list of column names, this method returns an array containing
     * the indexes of the specified columns.
     *
     * @param columnNames a list of column names in the schema
     *
     * @return an array containing the indexes of the columns specified in the
     *         input
     *
     * @throws SchemaNameException if a column name is specified multiple
     *         times in the input list, or if a column name doesn't appear in
     *         the schema
     */
    public int[] getColumnIndexes(List<String> columnNames) {
        int[] result = new int[columnNames.size()];
        HashSet<String> s = new HashSet<String>();

        int i = 0;
        for (String colName : columnNames) {
            if (!s.add(colName)) {
                throw new SchemaNameException(String.format(
                    "Column %s was specified multiple times", colName));
            }

            result[i] = getColumnIndex(colName);
            if (result[i] == -1) {
                throw new SchemaNameException(String.format(
                    "Schema doesn't contain a column named %s", colName));
            }

            i++;
        }

        return result;
    }


    /**
     * Given a (possibly wildcard) column-name, this method returns the collection
     * of all columns that match the specified column name.  The collection is a
     * mapping from integer indexes (the keys) to <code>ColumnInfo</code> objects
     * from the schema.
     * <p>
     * Any valid column-name object will work, so all of these options are
     * available:
     * <ul>
     *   <li><b>No table, only a column name</b> - to resolve an unqualified
     *     column name, e.g. in an expression or predicate</li>
     *   <li><b>A table and column name</b> - to check whether the schema contains
     *     such a column</li>
     *   <li><b>A wildcard without a table name</b> - to retrieve all columns in
     *     the schema</li>
     *   <li><b>A wildcard with a table name</b> - to retrieve all columns
     *     associated with a particular table name</li>
     * </ul>
     */
    public SortedMap<Integer, ColumnInfo> findColumns(ColumnName colName) {

        TreeMap<Integer, ColumnInfo> found = new TreeMap<Integer, ColumnInfo>();

        if (colName.isColumnWildcard()) {
            // Some kind of wildcard column-name object.

            if (!colName.isTableSpecified()) {
                // Wildcard with no table name:  *
                // Add all columns in the schema to the result.

                for (int i = 0; i < columnInfos.size(); i++)
                    found.put(i, columnInfos.get(i));
            }
            else {
                // Wildcard with a table name:  tbl.*
                // Find the table info and add its columns to the result.

                HashMap<String, IndexedColumnInfo> tableCols =
                    colsHashedByTable.get(colName.getTableName());

                if (tableCols != null) {
                    for (IndexedColumnInfo indexedColInfo: tableCols.values())
                        found.put(indexedColInfo.colIndex, indexedColInfo.colInfo);
                }
            }
        }
        else {
            // A non-wildcard column-name object.

            if (!colName.isTableSpecified()) {
                // Column name with no table name:  col
                // Look up the list of column-info objects grouped by column name.

                ArrayList<IndexedColumnInfo> colList =
                    colsHashedByColumn.get(colName.getColumnName());

                if (colList != null) {
                    for (IndexedColumnInfo indexedColInfo : colList)
                        found.put(indexedColInfo.colIndex, indexedColInfo.colInfo);
                }
            }
            else {
                // Column name with a table name:  tbl.col
                // Find the table info and see if it has the specified column.

                HashMap<String, IndexedColumnInfo> tableCols =
                    colsHashedByTable.get(colName.getTableName());

                if (tableCols != null) {
                    IndexedColumnInfo indexedColInfo =
                        tableCols.get(colName.getColumnName());
                    if (indexedColInfo != null)
                        found.put(indexedColInfo.colIndex, indexedColInfo.colInfo);
                }
            }
        }

        return found;
    }


    public String toString() {
        return "Schema[cols=" + columnInfos.toString() + "]";
    }
}
