package edu.caltech.nanodb.expressions;


import org.apache.commons.lang.ObjectUtils;


/**
 * This class represents a column name that appears in an SQL expression.  The
 * name may be a simple column name (e.g. "<tt>price</tt>"), a table name and
 * column name (e.g. "<tt>product.price</tt>"), or it may be a wildcard
 * specifier with or without the table name (e.g. "<tt>*</tt>" or
 * "<tt>product.*</tt>").
 */
public class ColumnName implements Comparable<ColumnName>, Cloneable {

    /**
     * The name of the table that the column name (or wildcard) is associated
     * with.  If this value is <code>null</code> then the table name is not
     * specified.
     */
    private String tableName;


    /**
     * The name of the column, or if this value is <code>null</code> then it
     * represents a wildcard value <code>*</code>.
     */
    private String columnName;


    public ColumnName(String tableName, String columnName) {
        this.tableName = tableName;
        this.columnName = columnName;
    }


    public ColumnName(String columnName) {
        this(null, columnName);
    }


    /**
     * Creates a new object corresponding to the wildcard column specifier
     * "<tt>*</tt>".
     */
    public ColumnName() {
        this(null, null);
    }


    @Override
    public boolean equals(Object obj) {
        if (obj instanceof ColumnName) {
            ColumnName other = (ColumnName) obj;

            return ObjectUtils.equals(tableName, other.tableName) &&
                   ObjectUtils.equals(columnName, other.columnName);
        }

        return false;
    }


    @Override
    public int hashCode() {
        int hash = 17;
        hash = 37 * hash + (tableName != null ? tableName.hashCode() : 0);
        hash = 37 * hash + (columnName != null ? columnName.hashCode() : 0);
        return hash;
    }


    @Override
    public Object clone() throws CloneNotSupportedException {
        return super.clone();
    }


    public int compareTo(ColumnName other) {
        if (tableName == null) {
            if (other.tableName != null)
                return -1;
        }
        else { // tableName != null
            if (other.tableName == null)
                return 1;

            int comp = tableName.compareTo(other.tableName);
            if (comp != 0)
                return comp;
        }

        if (columnName == null) {
            if (other.columnName != null)
                return -1;
        }
        else { // columnName != null
            if (other.columnName == null)
                return 1;

            int comp = columnName.compareTo(other.columnName);
            if (comp != 0)
                return comp;
        }

        return 0;
    }


    public String getTableName() {
        return tableName;
    }


    public void setTableName(String tableName) {
        this.tableName = tableName;
    }


    public String getColumnName() {
        return columnName;
    }


    public void setColumnName(String columnName) {
        this.columnName = columnName;
    }


    public boolean isTableSpecified() {
        return (tableName != null);
    }


    public boolean isColumnWildcard() {
        return (columnName == null);
    }


    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();

        if (tableName != null)
            sb.append(tableName).append('.');

        if (columnName != null)
            sb.append(columnName);
        else
            sb.append('*');

        return sb.toString();
    }
}
