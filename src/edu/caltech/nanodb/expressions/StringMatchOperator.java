package edu.caltech.nanodb.expressions;


import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import org.apache.log4j.Logger;

import edu.caltech.nanodb.relations.ColumnInfo;
import edu.caltech.nanodb.relations.ColumnType;
import edu.caltech.nanodb.relations.Schema;
import edu.caltech.nanodb.relations.SchemaNameException;


/**
 * <p>
 * This class implements string matching operations.  The supported operations
 * are:
 * </p>
 * <ul>
 *   <li>basic SQL pattern matching, <tt>a [NOT] LIKE b</tt></li>
 *   <li>regex pattern matching, <tt>a [NOT] SIMILAR TO b</tt></li>
 * </ul>
 * <p>
 * The <tt>a NOT LIKE ...</tt> and <tt>a NOT SIMILAR TO ...</tt> operation is
 * translated into <tt>NOT (a LIKE ...)</tt> etc. by the parser.
 * </p>
 */
public class StringMatchOperator extends Expression {

    /** A logging object for reporting anything interesting that happens. **/
    private static Logger logger = Logger.getLogger(StringMatchOperator.class);


    /**
     * This enumeration specifies the types of matching that can be performed.
     */
    public enum Type {
        LIKE("LIKE"),
        REGEX("SIMILAR TO");

        /** The string representation for each operator.  Used for printing. */
        private final String stringRep;

        /**
         * Construct a Type enum with the specified string representation.
         *
         * @param rep the string representation of the comparison type
         */
        Type(String rep) {
            stringRep = rep;
        }

        /**
         * Accessor for the operator type's string representation.
         *
         * @return the string representation of the comparison type
         */
        public String stringRep() {
            return stringRep;
        }
    }


    /** The kind of comparison, such as "LIKE" or "SIMILAR TO." */
    Type type;


    /** The left expression in the comparison. */
    Expression leftExpr;

    /** The right expression in the comparison. */
    Expression rightExpr;


    public StringMatchOperator(Type type, Expression lhs, Expression rhs) {
        if (type == null || lhs == null || rhs == null)
            throw new NullPointerException();

        this.type = type;
        leftExpr = lhs;
        rightExpr = rhs;
    }


    public ColumnInfo getColumnInfo(Schema schema) throws SchemaNameException {
        // Comparisons always return Boolean values, so just pass a Boolean
        // value in to the TypeConverter to get out the corresponding SQL type.
        ColumnType colType =
            new ColumnType(TypeConverter.getSQLType(Boolean.FALSE));
        return new ColumnInfo(colType);
    }


    /**
     * This helper method converts a SQL <tt>LIKE</tt> match-expression into
     * a corresponding regular expression.  The challenge is that the match
     * expression might contain regular-expression characters, so characters
     * other than "%" and "_" must be quoted so they don't cause any effects.
     *
     * @param sqlLike the SQL <tt>LIKE</tt> match-expression
     *
     * @return a regular expression corresponding to the match-expression
     */
    private String sqlLikeToRegex(String sqlLike) {
        StringBuilder buf = new StringBuilder();

        boolean inText = false;
        for (int i = 0; i < sqlLike.length(); i++) {
            char ch = sqlLike.charAt(i);
            if (ch == '%') {
                if (inText)
                    buf.append("\\E");

                buf.append(".*");
                inText = false;
            }
            else if (ch == '_') {
                if (inText)
                    buf.append("\\E");

                buf.append(".");
                inText = false;
            }
            else {
                if (!inText)
                    buf.append("\\Q");

                buf.append(ch);
                inText = true;
            }
        }
        if (inText)
            buf.append("\\E");

        return buf.toString();
    }


    /**
     * Evaluates this comparison expression and returns either
     * {@link java.lang.Boolean#TRUE} or {@link java.lang.Boolean#FALSE}.  If
     * either the left-hand or right-hand expression evaluates to
     * <code>null</code> (representing the SQL <tt>NULL</tt> value), then the
     * expression's result is always <code>FALSE</code>.
     */
    public Object evaluate(Environment env) throws ExpressionException {

        // Evaluate the left and right subexpressions, and coerce them into
        // strings.
        String lhsValue = TypeConverter.getStringValue(leftExpr.evaluate(env));
        String rhsValue = TypeConverter.getStringValue(rightExpr.evaluate(env));

        // If either the LHS value or RHS value is NULL (represented by Java
        // null value) then the entire expression evaluates to FALSE.
        if (lhsValue == null || rhsValue == null)
            return null;

        boolean result;

        switch (type) {
            case LIKE:
                // Convert the LIKE pattern into a regular expression.  First,
                // escape all the characters that could be meaningful in
                // regex, and then replace the SQL wildcard expressions with
                // corresponding regex expressions.
                String rhsRegex = sqlLikeToRegex(rhsValue);
                rhsRegex = rhsRegex.replaceAll("_", ".").replaceAll("%", ".*");

                logger.info("Received LIKE pattern \"" + rhsValue +
                    "\", converted to regex \"" + rhsRegex + "\".");

                result = Pattern.matches(rhsRegex, lhsValue);
                break;

            case REGEX:
                try {
                    result = Pattern.matches(rhsValue, lhsValue);
                }
                catch (PatternSyntaxException e) {
                    throw new ExpressionException("Regex pattern \"" +
                        rhsValue + "\" is invalid!", e);
                }
                break;

            default:
                throw new ExpressionException(
                    "Unrecognized string-matching type " + type);
        }

        return Boolean.valueOf(result);
    }


    @Override
    public Expression traverse(ExpressionProcessor p) {
        p.enter(this);
        leftExpr = leftExpr.traverse(p);
        rightExpr = rightExpr.traverse(p);
        return p.leave(this);
    }


    /**
     * Returns a string representation of this comparison expression and its
     * subexpressions.
     */
    public String toString() {
        // Convert all of the components into string representations.
        String leftStr = leftExpr.toString();
        String rightStr = rightExpr.toString();
        String opStr = " " + type.stringRep() + " ";

        // For now, assume we don't need parentheses.

        return leftStr + opStr + rightStr;
    }


    /**
     * Returns the type of this comparison operator.
     *
     * @return the type of comparison
     */
    public Type getType() {
        return type;
    }


    /**
     * Returns the left expression.
     *
     * @return the left expression
     */
    public Expression getLeftExpression() {
        return leftExpr;
    }


    /**
     * Returns the right expression.
     *
     * @return the right expression
     */
    public Expression getRightExpression() {
        return rightExpr;
    }


    /**
     * Checks if the argument is an expression with the same structure, but not
     * necessarily the same references.
     *
     * @param obj the object to which we are comparing
     */
    @Override
    public boolean equals(Object obj) {

        if (obj instanceof StringMatchOperator) {
            StringMatchOperator other = (StringMatchOperator) obj;

            return (type == other.type &&
                leftExpr.equals(other.leftExpr) &&
                rightExpr.equals(other.rightExpr));
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
        hash = 31 * hash + type.hashCode();
        hash = 31 * hash + leftExpr.hashCode();
        hash = 31 * hash + rightExpr.hashCode();
        return hash;
    }


    /**
     * Creates a copy of expression.  This method is used by the
     * {@link Expression#duplicate} method to make a deep copy of an expression
     * tree.
     */
    @Override
    protected Object clone() throws CloneNotSupportedException {
        StringMatchOperator expr = (StringMatchOperator) super.clone();

        // Type is immutable; copy it.
        expr.type = type;

        // Clone the subexpressions
        expr.leftExpr = (Expression) leftExpr.clone();
        expr.rightExpr = (Expression) rightExpr.clone();

        return expr;
    }
}
