package edu.caltech.nanodb.expressions;


import edu.caltech.nanodb.relations.ColumnInfo;
import edu.caltech.nanodb.relations.ColumnType;
import edu.caltech.nanodb.relations.Schema;
import edu.caltech.nanodb.relations.SchemaNameException;


/**
 * This class implements simple binary comparison operations.  The supported
 * operations are:
 * <ul>
 *   <li>equals, <tt>=</tt></li>
 *   <li>not-equals, <tt>!=</tt> or <tt>&lt;&gt;</tt></li>
 *   <li>greater-than, <tt>&gt;</tt></li>
 *   <li>less-than, <tt>&lt;</tt></li>
 *   <li>greater-or-equal, <tt>&gt;=</tt></li>
 *   <li>less-or-equal, <tt>&lt;=</tt></li>
 * </ul>
 */
public class CompareOperator extends Expression {
    /**
     * This enumeration specifies the types of comparisons that can be
     * performed.
     */
    public enum Type {
        EQUALS("=="),
        NOT_EQUALS("!="),
        LESS_THAN("<"),
        GREATER_THAN(">"),
        LESS_OR_EQUAL("<="),
        GREATER_OR_EQUAL(">=");

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

    /** The kind of comparison, such as "equals" or "less than." */
    Type type;

    /** The left expression in the comparison. */
    Expression leftExpr;

    /** The right expression in the comparison. */
    Expression rightExpr;


    public CompareOperator(Type type, Expression lhs, Expression rhs) {
        if (type == null || lhs == null || rhs == null)
            throw new NullPointerException();

        leftExpr = lhs;
        rightExpr = rhs;

        this.type = type;
    }


    public ColumnInfo getColumnInfo(Schema schema) throws SchemaNameException {
        // Comparisons always return Boolean values, so just pass a Boolean
        // value in to the TypeConverter to get out the corresponding SQL type.
        ColumnType colType =
            new ColumnType(TypeConverter.getSQLType(Boolean.FALSE));
        return new ColumnInfo(colType);
    }


    /**
     * Evaluates this comparison expression and returns either
     * {@link java.lang.Boolean#TRUE} or {@link java.lang.Boolean#FALSE}.  If
     * either the left-hand or right-hand expression evaluates to
     * <code>null</code> (representing the SQL <tt>NULL</tt> value), then the
     * expression's result is always <code>FALSE</code>.
     *
     * @design (Donnie) We have to suppress "unchecked operation" warnings on
     *         this code, since {@link Comparable} is a generic (and thus allows
     *         us to specify the type of object being compared), but we want to
     *         use it without specifying any types.
     */
    @SuppressWarnings("unchecked")
    public Object evaluate(Environment env) throws ExpressionException {

        // Evaluate the left and right subexpressions.
        Object lhsValue = leftExpr.evaluate(env);
        Object rhsValue = rightExpr.evaluate(env);

        // If either the LHS value or RHS value is NULL (represented by Java null
        // value) then the entire expression evaluates to FALSE.
        if (lhsValue == null || rhsValue == null)
            return null;

        // Coerce the values to have the same type, then do the comparison.

        TypeConverter.Pair coerced =
            TypeConverter.coerceComparison(lhsValue, rhsValue);

        Comparable lhsComp = (Comparable) coerced.value1;
        Comparable rhsComp = (Comparable) coerced.value2;

        int compResult = lhsComp.compareTo(rhsComp);

        boolean result;

        switch (type) {
        case EQUALS:
            result = (compResult == 0);
            break;

        case NOT_EQUALS:
            result = (compResult != 0);
            break;

        case LESS_THAN:
            result = (compResult < 0);
            break;

        case GREATER_THAN:
            result = (compResult > 0);
            break;

        case LESS_OR_EQUAL:
            result = (compResult <= 0);
            break;

        case GREATER_OR_EQUAL:
            result = (compResult >= 0);
            break;

        default:
            throw new ExpressionException("Unrecognized comparison type " + type);
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


    public void normalize() {
        if (leftExpr instanceof LiteralValue && rightExpr instanceof ColumnValue) {
            // Switch left and right expressions.
            Expression temp = leftExpr;
            leftExpr = rightExpr;
            rightExpr = temp;

            // Adjust the operator to properly reflect the reordering.
            switch (type) {
            case GREATER_THAN:
                type = CompareOperator.Type.LESS_THAN;
                break;

            case LESS_THAN:
                type = CompareOperator.Type.GREATER_THAN;
                break;

            case GREATER_OR_EQUAL:
                type = CompareOperator.Type.LESS_OR_EQUAL;
                break;

            case LESS_OR_EQUAL:
                type = CompareOperator.Type.GREATER_OR_EQUAL;
                break;

            default:
                // No changes needed.
            }
        }
    }


    /**
     * Checks if the argument is an expression with the same structure, but not
     * necessarily the same references.
     *
     * @param obj the object to which we are comparing
     */
    @Override
    public boolean equals(Object obj) {

        if (obj instanceof CompareOperator) {
            CompareOperator other = (CompareOperator) obj;

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
        CompareOperator expr = (CompareOperator) super.clone();

        // Type is immutable; copy it.
        expr.type = type;

        // Clone the subexpressions
        expr.leftExpr = (Expression) leftExpr.clone();
        expr.rightExpr = (Expression) rightExpr.clone();

        return expr;
    }


    /**
     * Performs an equality comparison between two objects, using the
     * database's rules for type conversions/coercions.
     *
     * @param obj1 the first object to compare
     * @param obj2 the second object to compare
     * @return the results of the equality comparison
     */
    public static boolean areObjectsEqual(Object obj1, Object obj2) {
        TypeConverter.Pair pair =
            TypeConverter.coerceComparison(obj1, obj2);

        return pair.value1.equals(pair.value2);
    }
}
