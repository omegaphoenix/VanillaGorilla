package edu.caltech.nanodb.commands;


import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import edu.caltech.nanodb.relations.ForeignKeyValueChangeOption;
import edu.caltech.nanodb.relations.TableConstraintType;


/**
 * Constraints may be specified at the table level, or they may be specified
 * on individual columns.  Obviously, the kinds of constraint allowed depends
 * on what the constraint is associated with.
 */
public class ConstraintDecl {

    /**
     * The optional name of the constraint, or <code>null</code> if no name was
     * specified.
     */
    private String name = null;


    /** The type of the constraint. */
    private TableConstraintType type;


    /**
     * Flag indicating whether the constraint is specified at the table-level or
     * at the column-level.  A value of <tt>true</tt> indicates that it is a
     * column-level constraint; a value of <tt>false</tt> indicates that it is a
     * table-level constraint.
     */
    private boolean columnConstraint;


    /**
     * For {@link TableConstraintType#UNIQUE} and
     * {@link TableConstraintType#PRIMARY_KEY} constraints, this is a list of
     * one or more column names that are constrained.  Note that for a column
     * constraint, this list will contain <i>exactly</i> one column name.
     * For a table-level constraint, this list will contain one or more column
     * names.
     * <p>
     * For any other constraint type, this will be set to <tt>null</tt>.
     */
    private List<String> columnNames = new ArrayList<String>();


    /**
     * For {@link TableConstraintType#FOREIGN_KEY} constraints, this is the
     * table that is referenced by the column.
     * <p>
     * For any other constraint type, this will be set to <tt>null</tt>.
     */
    private String refTableName = null;


    /**
     * For {@link TableConstraintType#FOREIGN_KEY} constraints, this is a list
     * of one or more column names in the reference-table that are referenced
     * by the foreign-key constraint.  Note that for a column-constraint, this
     * list will contain <i>exactly</i> one column-name.  For a
     * table-constraint, this list will contain one or more column-names.
     * <p>
     * For any other constraint type, this will be set to <code>null</code>.
     */
    private List<String> refColumnNames = new ArrayList<String>();


    /**
     * For {@link TableConstraintType#FOREIGN_KEY} constraints, this is the
     * {@link edu.caltech.nanodb.relations.ForeignKeyValueChangeOption} for
     * <tt>ON DELETE</tt> behavior.
     * <p>
     * For any other constraint type, this will be set to <tt>null</tt>.
     */
    private ForeignKeyValueChangeOption onDeleteOption = null;


    /**
     * For {@link TableConstraintType#FOREIGN_KEY} constraints, this is the
     * {@link edu.caltech.nanodb.relations.ForeignKeyValueChangeOption} for
     * <tt>ON UPDATE</tt> behavior.
     * <p>
     * For any other constraint type, this will be set to <tt>null</tt>.
     */
    private ForeignKeyValueChangeOption onUpdateOption = null;


    /** Create a new unnamed constraint for a table or a table-column. */
    public ConstraintDecl(TableConstraintType type, boolean columnConstraint) {
        this(type, null, columnConstraint);
    }


    /** Create a new named constraint for a table or a table-column. */
    public ConstraintDecl(TableConstraintType type, String name,
                          boolean columnConstraint) {
        this.type = type;
        this.name = name;
        this.columnConstraint = columnConstraint;
    }


    public String getName() {
        return name;
    }


    /** Returns the type of this constraint. */
    public TableConstraintType getType() {
        return type;
    }


    /**
     * Returns <tt>true</tt> if this constraint is associated with a
     * table-column, or <tt>false</tt> if it is a table-level constraint.
     */
    public boolean isColumnConstraint() {
        return columnConstraint;
    }


    /**
     * Add a column to the constraint.  This specifies that the constraint
     * governs values in the column.  For column-level constraints, only a
     * single column may be specified.  For table-level constraints, one or more
     * columns may be specified.
     *
     * @param columnName the column governed by the constraint
     *
     * @throws NullPointerException if columnName is <tt>null</tt>
     * @throws IllegalStateException if this is a column-constraint and
     *         there is already one column specified
     *
     * @design (donnie) Column names are checked for existence and uniqueness
     *         when initializing the corresponding objects for storage on the
     *         table schema.  See
     *         {@link edu.caltech.nanodb.relations.TableSchema#addCandidateKey} and
     *         {@link edu.caltech.nanodb.relations.TableSchema#addForeignKey}
     *         for details.
     */
    public void addColumn(String columnName) {
        if (columnName == null)
            throw new NullPointerException("columnName");

        if (columnNames.size() == 1 && isColumnConstraint()) {
            throw new IllegalStateException(
                "Cannot specify multiple columns in a column-constraint.");
        }

        columnNames.add(columnName);
    }


    public List<String> getColumnNames() {
        return Collections.unmodifiableList(columnNames);
    }


    /**
     * Add a reference-table to a {@link TableConstraintType#FOREIGN_KEY}
     * constraint.  This specifies the table that constrains the values in
     * the column.
     *
     * @param tableName the table referenced in the constraint
     *
     * @throws NullPointerException if tableName is <tt>null</tt>
     * @throws IllegalStateException if this constraint is not a foreign-key
     *         constraint
     *
     * @design (donnie) Existence of the referenced table is checked in the
     *         {@link CreateTableCommand#execute} method's operation.
     */
    public void setRefTable(String tableName) {
        if (type != TableConstraintType.FOREIGN_KEY) {
            throw new IllegalStateException(
                "Reference tables are only specified on FOREIGN_KEY constraints.");
        }

        if (tableName == null)
            throw new IllegalArgumentException("tableName must be specified");

        refTableName = tableName;
    }


    /**
     * Returns the name of the referenced table for a
     * {@link TableConstraintType#FOREIGN_KEY} constraint.
     *
     * @return the name of the referenced table for a FOREIGN_KEY constraint.
     */
    public String getRefTable() {
        if (type != TableConstraintType.FOREIGN_KEY) {
            throw new IllegalStateException(
                "Reference tables are only specified on FOREIGN_KEY constraints.");
        }
        return refTableName;
    }


    /**
     * Add a reference-column to a {@link TableConstraintType#FOREIGN_KEY}
     * constraint.  This specifies the column that constrains the values in
     * the column.  For column-level constraints, only a single column may be
     * specified.  For table-level constraints, one or more columns may be
     * specified.
     *
     * @param columnName the column referenced in the constraint
     *
     * @throws NullPointerException if columnName is <tt>null</tt>
     * @throws IllegalStateException if this constraint is not a foreign-key
     *         constraint, or if this is a column-constraint and there is
     *         already one reference-column specified
     *
     * @design (donnie) Column names are checked for existence and uniqueness
     *         when initializing the corresponding objects for storage on the
     *         table schema.  See
     *         {@link edu.caltech.nanodb.relations.TableSchema#addCandidateKey} and
     *         {@link edu.caltech.nanodb.relations.TableSchema#addForeignKey}
     *         for details.
     */
    public void addRefColumn(String columnName) {
        if (type != TableConstraintType.FOREIGN_KEY) {
            throw new IllegalStateException(
                "Reference columns only allowed on FOREIGN_KEY constraints.");
        }

        if (columnName == null)
            throw new NullPointerException("columnName");

        if (refColumnNames.size() == 1 && isColumnConstraint()) {
            throw new IllegalStateException("Cannot specify multiple " +
                "reference-columns in a column-constraint.");
        }

        refColumnNames.add(columnName);
    }


    public List<String> getRefColumnNames() {
        return Collections.unmodifiableList(refColumnNames);
    }


    /**
     * Add an <tt>ON DELETE</tt> option to a
     * {@link TableConstraintType#FOREIGN_KEY} constraint.
     *
     * @param f the {@link ForeignKeyValueChangeOption} to set for
     *        <tt>ON DELETE</tt> behavior
     *
     * @throws IllegalStateException if this constraint is not a foreign-key
     *         constraint
     */
    public void setOnDeleteOption(ForeignKeyValueChangeOption f) {
        if (type != TableConstraintType.FOREIGN_KEY) {
            throw new IllegalStateException(
                "ON DELETE only specified on FOREIGN_KEY constraints.");
        }

        onDeleteOption = f;
    }


    /**
     * Returns the <tt>ON DELETE</tt> {@link ForeignKeyValueChangeOption} for
     * a {@link TableConstraintType#FOREIGN_KEY} constraint.
     *
     * @return the <tt>ON DELETE</tt> {@link ForeignKeyValueChangeOption}
     */
    public ForeignKeyValueChangeOption getOnDeleteOption() {
        if (type != TableConstraintType.FOREIGN_KEY) {
            throw new IllegalStateException(
                "ON DELETE only specified on FOREIGN_KEY constraints.");
        }
        return onDeleteOption;
    }


    /**
     * Add an <tt>ON UPDATE</tt> option to a
     * {@link TableConstraintType#FOREIGN_KEY} constraint.
     *
     * @param f the {@link ForeignKeyValueChangeOption} to set for
     *        <tt>ON UPDATE</tt> behavior
     *
     * @throws IllegalStateException if this constraint is not a foreign-key
     *         constraint
     */
    public void setOnUpdateOption(ForeignKeyValueChangeOption f) {
        if (type != TableConstraintType.FOREIGN_KEY) {
            throw new IllegalStateException(
                "ON UPDATE only specified on FOREIGN_KEY constraints.");
        }

        onUpdateOption = f;
    }


    /**
     * Returns the <tt>ON UPDATE</tt> {@link ForeignKeyValueChangeOption} for
     * a {@link TableConstraintType#FOREIGN_KEY} constraint.
     *
     * @return the <tt>ON UPDATE</tt> {@link ForeignKeyValueChangeOption}
     */
    public ForeignKeyValueChangeOption getOnUpdateOption() {
        if (type != TableConstraintType.FOREIGN_KEY) {
            throw new IllegalStateException(
                "ON UPDATE only specified on FOREIGN_KEY constraints.");
        }
        return onUpdateOption;
    }


    @Override
    public String toString() {
        return "Constraint[" + (name != null ? name : "(unnamed)") + " : " +
            type + ", " + (isColumnConstraint() ? "column" : "table") + "]";
    }
}
