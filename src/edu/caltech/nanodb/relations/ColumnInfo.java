package edu.caltech.nanodb.relations;


import org.apache.commons.lang.ObjectUtils;

import edu.caltech.nanodb.expressions.ColumnName;

import java.io.Serializable;


/**
 * Basic information about a table column, including its name and SQL type.
 * Constraints, even "<tt>NOT NULL</tt>" constraints, appear at the table level,
 * since some constraints can involve multiple columns.
 *
 * @design (donnie) Right now this class is immutable, which I tend to think is
 *         a good idea for schema information.  So far there aren't any classes
 *         that really require the ability to mutate column-info objects,
 *         although the biggest candidates are the
 *         {@link edu.caltech.nanodb.expressions.TupleLiteral} class and the
 *         {@link edu.caltech.nanodb.plannodes.ProjectNode} class, which sometimes
 *         need to generate names for their results.
 */
public class ColumnInfo implements Serializable {

    /** The name of the attribute. */
    private String name;


    /**
     * An optional table-name for the attribute, in cases where a join or
     * Cartesian product generates a result with duplicate attribute-names.  In
     * most cases it is expected that this table-name will be <tt>null</tt>.
     */
    private String tableName;


    /** The type information for the column. */
    private ColumnType type;


    /**
     * Construct a new column-info object for a column, specifying the attribute
     * name, table name, and type of the column.
     *
     * @param name the attribute name, or {@code null} if unspecified
     * @param tableName the table name, or {@code null} if unspecified
     * @param type the type of the column
     */
    public ColumnInfo(String name, String tableName, ColumnType type) {
        if (type == null)
            throw new NullPointerException("type cannot be null");

        this.name = name;
        this.tableName = tableName;
        this.type = type;
    }


    /**
     * Construct a new column-info object for a column, specifying the attribute
     * name and type of the column.  The table name is unspecified.
     *
     * @param name the attribute name, or <tt>null</tt> if unspecified
     * @param type the type of the column
     */
    public ColumnInfo(String name, ColumnType type) {
        this(name, null, type);
    }


    /**
     * Construct a new column-info object for a column, specifying the type
     * of the column.  The attribute name and table name are unspecified.
     *
     * @param type the type of the column
     */
    public ColumnInfo(ColumnType type) {
        this(null, null, type);
    }


    /**
     * Returns the name of the attribute.
     *
     * @return the name of the attribute
     */
    public String getName() {
        return name;
    }


    /**
     * Returns the table-name of the attribute.
     *
     * @return the table-name of the attribute
     */
    public String getTableName() {
        return tableName;
    }


    /**
     * Returns a {@link ColumnName} object representing the name of this column.
     *
     * @return a column-name object for this column
     */
    public ColumnName getColumnName() {
        return new ColumnName(tableName, name);
    }


    /**
     * Returns the type of the attribute.
     *
     * @return the type of the attribute
     */
    public ColumnType getType() {
        return type;
    }


    @Override
    public boolean equals(Object obj) {
        if (obj instanceof ColumnInfo) {
            ColumnInfo other = (ColumnInfo) obj;

            return ObjectUtils.equals(name, other.name) &&
                   ObjectUtils.equals(tableName, other.tableName) &&
                   type == other.type;
        }
        return false;
    }


    @Override
    public int hashCode() {
        int hash = 7;
        hash = 13 * hash + (name != null ? name.hashCode() : 0);
        hash = 13 * hash + (tableName != null ? tableName.hashCode() : 0);
        hash = 13 * hash + type.hashCode();
        return hash;
    }


    /** Returns a string representation of the column-info. */
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();

        sb.append("ColumnInfo[");

        if (tableName != null)
            sb.append(tableName).append('.');

        sb.append(name).append(':').append(type).append(']');

        return sb.toString();
    }
}
