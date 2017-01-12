package edu.caltech.nanodb.expressions;


import org.apache.commons.lang.ObjectUtils;

import edu.caltech.nanodb.relations.ColumnInfo;
import edu.caltech.nanodb.relations.ColumnType;
import edu.caltech.nanodb.relations.Schema;
import edu.caltech.nanodb.relations.SchemaNameException;


/**
 * This expression class represents literal values.  The value is stored
 * as a {@link java.lang.Object} to avoid complexity in the expression
 * processing code, but it requires additional type checking and
 * conversion, which makes it a bit slower than type-specific literal
 * classes would be.
 */
public class LiteralValue extends Expression {

    /** The literal value of this expression. */
    private Object value;


    public LiteralValue(Object value) {
        this.value = value;
    }


    public ColumnInfo getColumnInfo(Schema schema) throws SchemaNameException {
        ColumnType colType = new ColumnType(TypeConverter.getSQLType(value));
        return new ColumnInfo(toString(), colType);
    }


    /**
     * For literal values, evaluation simply involves returning the literal
     * value.
     */
    public Object evaluate(Environment env) {
        return value;
    }


    @Override
    public Expression traverse(ExpressionProcessor p) {
        p.enter(this);
        return p.leave(this);
    }


    @Override
    public String toString() {
        String result;
        if (value == null)
            result = "NULL";
        else if (value instanceof String)
            result = "'" + value + "'";
        else
            result = value.toString();

        return result;
    }


    /**
     * Literal values cannot be simplified any further, so this method just
     * returns the expression it's called on.
     */
    public Expression simplify() {
        return this;
    }
  
  
    /**
     * Checks if the argument is an expression with the same structure, but not
     * necessarily the same references.
     *
     * @design This function operates correctly on the assumption that all
     *         supported types override Object.equals().
     *
     * @param obj the object to which we are comparing
     */
    @Override
    public boolean equals(Object obj) {
        if (obj instanceof LiteralValue) {
            LiteralValue other = (LiteralValue) obj;
            return ObjectUtils.equals(value, other.value);
        }
        return false;
    }


    /**
     * Computes the hashcode of an Expression.  This method is used to see if
     * two expressions might be equal.
     */
    @Override
    public int hashCode() {
        int hash = 7;
        hash = 31 * hash + (value != null ? value.hashCode() : 0);
        return hash;
    }


    /**
     * Creates a copy of expression.
     *
     * @design The reference of the internal value is simply copied since the
     *         value types are all immutable.
     */
    @Override
    protected Object clone() throws CloneNotSupportedException {
        LiteralValue expr = (LiteralValue)super.clone();

        // LiteralValue copies share the same reference because the value-object
        // won't be mutated in place.
        expr.value = value;

        return expr;
    }
}
