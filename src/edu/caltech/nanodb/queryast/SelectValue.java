package edu.caltech.nanodb.queryast;


import java.util.ArrayList;
import java.util.List;

import edu.caltech.nanodb.expressions.ColumnName;
import edu.caltech.nanodb.expressions.ColumnValue;
import edu.caltech.nanodb.expressions.Expression;

import edu.caltech.nanodb.relations.ColumnInfo;
import edu.caltech.nanodb.relations.Schema;

import org.apache.commons.lang.ObjectUtils;


/**
 * This class represents a single expression in a <tt>SELECT</tt> clause.  The
 * expression can be one of the following:
 * <ul>
 *   <li>An {@link edu.caltech.nanodb.expressions.Expression} that evaluates
 *       to a single value</li>
 *   <li>A wildcard expression like "<tt>*</tt>" or "<tt>loan.*</tt>" that
 *       evaluates to a set of column values</li>
 *   <li>A scalar subquery (often correlated) that evaluates to a single column
 *       and row</li>
 * </ul>
 * <p>
 * The {@link #isWildcard} method can be used to determine if an object of this
 * type represents a wildcard value.
 */
public class SelectValue implements Cloneable {

    public static final String UNNAMED_COLUMN_PREFIX = "?unnamed";


    /**
     * If this select-value is a simple expression then this field will be
     * set to the expression.  Otherwise, this value will be {@code null}.
     */
    private Expression expression = null;


    /**
     * If this select-value is a scalar subquery then this field references the
     * subquery expression.  Otherwise, this value will be {@code null}.
     */
    private SelectClause scalarSubquery = null;


    /**
     * If this select-value is an expression or a scalar subquery, this field
     * will optionally specify an alias for the expression's value.  This is for
     * expressions such as "<tt>SELECT balance * interest AS new_balance
     * FROM loans</tt>".  For this example, the result-alias would be
     * "<tt>new_balance</tt>".
     */
    private String resultAlias = null;


    /**
     * If the <tt>SELECT</tt> clause includes a wildcard column specifier
     * (e.g. "<tt>*</tt>" or "<tt>product.*</tt>") then this field will
     * contain the details of that specifier.  If the expression doesn't
     * include a wildcard then this field will be <code>null</code>, and
     * the {@link #expression} field will specify the expression.
     */
    private ColumnName wildcardColumnName = null;


    /**
     * Construct a select-value object from an expression and an optional
     * alias or nickname value.
     *
     * @param e The expression that generates the select-value.
     * @param alias If not <code>null</code>, this is the column name to
     *        assign to the result value.
     *
     * @throws java.lang.NullPointerException if <code>e</code> is
     *         <code>null</code>
     */
    public SelectValue(Expression e, String alias) {
        if (e == null)
            throw new NullPointerException();

        expression = e;
        resultAlias = alias;
    }


    /**
     * Construct a select-value object from a wildcard column specification.
     *
     * @param colName The wildcard column name.
     *
     * @throws java.lang.NullPointerException if <code>colName</code> is
     *         <code>null</code>
     * @throws java.lang.IllegalArgumentException if <code>colName</code> is not a
     *         wildcard column name.
     */
    public SelectValue(ColumnName colName) {
        if (colName == null)
            throw new NullPointerException();

        if (!colName.isColumnWildcard()) {
            throw new IllegalArgumentException(
                "Non-wildcard column names should be represented as expressions.");
        }

        wildcardColumnName = colName;
    }



    public SelectValue(SelectClause subquery, String alias) {
        if (subquery == null)
            throw new NullPointerException();

        scalarSubquery = subquery;
        resultAlias = alias;
    }


    @Override
    public String toString() {
        StringBuilder buf = new StringBuilder();

        if (wildcardColumnName != null)
            buf.append(wildcardColumnName);
        else if (scalarSubquery != null)
            buf.append(scalarSubquery);
        else
            buf.append(expression);

        if (wildcardColumnName == null && resultAlias != null)
            buf.append(" AS ").append(resultAlias);

        return buf.toString();
    }


    /**
     * Returns true if this SelectValue is a wildcard expression, either of the
     * form "<tt>*</tt>" or "<tt>loan.*</tt>".
     *
     * @return true if this select expression is a wildcard expression.
     */
    public boolean isWildcard() {
        return (wildcardColumnName != null);
    }


    public boolean isExpression() {
        return (expression != null);
    }


    public boolean isSimpleColumnValue() {
        return (expression instanceof ColumnValue);
    }


    /**
     * Returns this SelectValue's expression or null if this is a wildcard.
     *
     * @return the expression that generates the select value
     */
    public Expression getExpression() {
        return expression;
    }


    public void setExpression(Expression e) {
        expression = e;
    }


    public boolean isScalarSubquery() {
        return (scalarSubquery != null);
    }


    /**
     * Returns this SelectValue's alias result name or <tt>null</tt> if it is
     * unspecified.
     *
     * @return the string alias, or <tt>null</tt> if unspecified
     */
    public String getAlias() {
        return resultAlias;
    }


    /**
     * Returns the wildcard {@link ColumnName} object or <tt>null</tt> if there
     * is none.
     *
     * @return the wildcard specification, or <tt>null</tt> if unspecified
     */
    public ColumnName getWildcard() {
        return wildcardColumnName;
    }


    public SelectClause getScalarSubquery() {
        return scalarSubquery;
    }


    /**
     * This function returns the column-details of all results that will be
     * produced by this select-value.  For example, if the select value is a
     * wildcard such as <tt>*</tt> or <tt>tbl.*</tt> then the referenced
     * column details will be retrieved from the input-schema.
     * <p>
     * If the select value is an expression then the result column-info is
     * determined from the expression itself.  For example, the expression
     * might simply be a column <tt>a</tt>, or <tt>tbl.a</tt>; in these cases
     * the column-info is retrieved from the input schema.  Expressions can
     * also be renamed; this renaming is applied here.  However, if a complex
     * expression doesn't have a name then it will be assigned a unique name.
     *
     * @param inputSchema the schema against which the select-value will be
     *        evaluated
     *
     * @param resultSchema the current result schema "so far".  This is
     *        provided so that if the select-value is unnamed then a unique
     *        name can be generated.
     *
     * @return a collection of one or more column-information objects
     *         containing the schema for this select-value.
     */
    public List<ColumnInfo> getColumnInfos(Schema inputSchema, Schema resultSchema) {
        ArrayList<ColumnInfo> results = new ArrayList<ColumnInfo>();

        if (wildcardColumnName != null) {
            // The values matching the wildcard column-name will appear in the
            // SELECT clause's output.
            results.addAll(inputSchema.findColumns(wildcardColumnName).values());
        }
        else if (expression != null) {
            ColumnInfo colInfo = expression.getColumnInfo(inputSchema);
            if (resultAlias != null) {
                // The result has an alias specified, so set the column name
                // to be that alias.
                colInfo = new ColumnInfo(resultAlias, colInfo.getType());
            }
            else if (colInfo.getName() == null) {
                // The column didn't have a name specified, so generate one
                // that is unique in the result schema.

                int i = 1;
                while (true) {
                    String name = UNNAMED_COLUMN_PREFIX + Integer.toString(i);
                    if (resultSchema.numColumnsWithName(name) > 0) {
                        i++;
                        continue;
                    }

                    colInfo = new ColumnInfo(name, colInfo.getType());
                    break;
                }
            }
            results.add(colInfo);
        }
        else if (scalarSubquery != null) {
            throw new UnsupportedOperationException(
                "Support for scalar subqueries is currently incomplete.");
        }
        else {
            throw new IllegalStateException(
                "Select-value doesn't specify any values");
        }

        return results;
    }


    /**
     * Checks if the argument is a SelectValue with the same fields.
     *
     * @param obj the object to which we are comparing
     */
    @Override
    public boolean equals(Object obj) {
        if (obj instanceof SelectValue) {
            SelectValue other = (SelectValue) obj;

            return ObjectUtils.equals(expression, other.expression) &&
                   ObjectUtils.equals(resultAlias, other.resultAlias) &&
                   ObjectUtils.equals(wildcardColumnName, other.wildcardColumnName);
        }

        return false;
    }


    /**
     * Computes the hashcode of a SelectValue.  This method is used to see if
     * two SelectValues CAN be equal.
     */
    @Override
    public int hashCode() {
        int hash = 7;

        hash = 31 * hash + (expression != null ? expression.hashCode() : 0);
        hash = 31 * hash + (resultAlias != null ? resultAlias.hashCode() : 0);
        hash = 31 * hash + (wildcardColumnName != null ?
                            wildcardColumnName.hashCode() : 0);

        return hash;
    }


    /** Creates a copy of a select value. */
    @Override
    public Object clone() throws CloneNotSupportedException {
        SelectValue sel = (SelectValue) super.clone();

        if (expression != null)
            sel.expression = expression.duplicate();

        // TODO:  For now, presume that the scalar subquery is immutable.
        if (scalarSubquery != null)
            sel.scalarSubquery = scalarSubquery;

        if (resultAlias != null)
            sel.resultAlias = resultAlias;      // Strings are immutable

        if (wildcardColumnName != null)
            sel.wildcardColumnName = (ColumnName) wildcardColumnName.clone();

        return sel;
    }
}
