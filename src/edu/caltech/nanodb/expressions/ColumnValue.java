package edu.caltech.nanodb.expressions;


import java.util.List;
import java.util.SortedMap;

import edu.caltech.nanodb.relations.ColumnInfo;
import edu.caltech.nanodb.relations.Schema;
import edu.caltech.nanodb.relations.SchemaNameException;
import edu.caltech.nanodb.relations.Tuple;


/**
 * This expression class represents the value of a tuple column.  The
 * column name is stored in the expression object, and the actual value
 * of the column is looked up during evaluation time.
 */
public class ColumnValue extends Expression {

    /** The name of the column. */
    private ColumnName columnName;


    /**
     * Initialize a new column-value expression object with the specified
     * column-name.
     *
     * @param columnName the name of the column to retrieve the value for
     */
    public ColumnValue(ColumnName columnName) {
        if (columnName == null)
            throw new NullPointerException();

        // We are allowed to specify wildcards now!
        // TODO: This may be wrong
        /* if (columnName.isColumnWildcard()) {
            throw new IllegalArgumentException(
            "Cannot specify wildcard for a column value; got " + columnName + ".");
        } */

        this.columnName = columnName;
    }


    /**
     * Returns the column name object
     *
     * @return the column name object
     */
    public ColumnName getColumnName() {
        return columnName;
    }


    /**
     * Sets the column name object
     * 
     * @param columnName the new column name object
     */
    public void setColumnName(ColumnName columnName) {
        this.columnName = columnName;
    }


    public ColumnInfo getColumnInfo(Schema schema) throws SchemaNameException {
        SortedMap<Integer, ColumnInfo> found = schema.findColumns(columnName);

        ColumnInfo colInfo;

        if (found.size() == 1) {
            colInfo = found.get(found.firstKey());
        }
        else if (found.size() == 0) {
            throw new SchemaNameException("Unknown column " + columnName + ".");
        }
        else {
            assert found.size() > 1;
            throw new SchemaNameException("Ambiguous column " + columnName +
                "; found " + found.values() + ".");
        }

        return colInfo;
    }


    public Object evaluate(Environment env) throws ExpressionException {
        if (columnName.isColumnWildcard()) {
            List<Tuple> tuples = env.getCurrentTuples();
            if (tuples.size() > 1) {
                throw new IllegalStateException(
                    "Can't evaluate * in the context of multiple tuples.");
            }

            return tuples.get(0);
        }

        return env.getColumnValue(columnName);
    }


    @Override
    public Expression traverse(ExpressionProcessor p) {
        // This is a leaf, so we just enter and leave the node.
        p.enter(this);
        return p.leave(this);
    }


    @Override
    public String toString() {
        return columnName.toString();
    }


    /**
     * Column values cannot be simplified any further, so this method just
     * returns the expression it's called on.
     */
    public Expression simplify() {
        return this;
    }
  
  
    /**
     * Checks if the argument is an expression with the same structure, but not
     * necessarily the same references.
     *
     * @param obj the object to which we are comparing
     */
    @Override
    public boolean equals(Object obj) {
        if (obj instanceof ColumnValue) {
            ColumnValue other = (ColumnValue) obj;
            return columnName.equals(other.columnName);
        }
        return false;
    }
  
  
    /**
     * Computes the hashcode of an Expression.  This method is used to see if
     * two expressions might be equal.
     */
    @Override
    public int hashCode() {
        // Since the only thing in a column-value is a column-name, just return
        // that object's hash-code.
        return columnName.hashCode();
    }


    /** Creates a copy of expression. */
    @Override
    protected Object clone() throws CloneNotSupportedException {
        ColumnValue expr = (ColumnValue) super.clone();

        // Copy the ColumnName object, since it can be mutated in place.
        expr.columnName = (ColumnName) columnName.clone();

        return expr;
    }
}
