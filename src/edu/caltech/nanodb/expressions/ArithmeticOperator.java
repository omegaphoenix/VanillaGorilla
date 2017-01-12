package edu.caltech.nanodb.expressions;


import edu.caltech.nanodb.relations.ColumnInfo;
import edu.caltech.nanodb.relations.ColumnType;
import edu.caltech.nanodb.relations.SQLDataType;
import edu.caltech.nanodb.relations.Schema;
import edu.caltech.nanodb.relations.SchemaNameException;


/**
 * This class implements simple binary arithmetic operations.  The supported
 * operations are:
 * <ul>
 *   <li>addition, <tt>+</tt></li>
 *   <li>subtraction, <tt>-</tt></li>
 *   <li>multiplication, <tt>*</tt></li>
 *   <li>division, <tt>/</tt></li>
 *   <li>remainder, <tt>%</tt></li>
 *   <li>exponentiation, <tt>^</tt></li>
 * </ul>
 */
public class ArithmeticOperator extends Expression {

    /**
     * This enum specifies the arithmetic operations that this class can provide.
     * Each arithmetic operation also holds its string representation, which is
     * used when converting an arithmetic expression into a string for display.
     */
    public enum Type {
        ADD("+"),
        SUBTRACT("-"),
        MULTIPLY("*"),
        DIVIDE("/"),
        REMAINDER("%"),
        POWER("^");


        /** The string representation for each operator.  Used for printing. */
        private final String stringRep;

        /**
         * Construct a Type enum with the specified string representation.
         *
         * @param rep the string representation of the arithmetic operation
         */
        Type(String rep) {
            stringRep = rep;
        }

        /**
         * Accessor for the operator type's string representation.
         *
         * @return the string representation of the arithmetic operation
         */
        public String stringRep() {
            return stringRep;
        }
    }


    /** The kind of comparison, such as "subtract" or "multiply." */
    Type type;

    /** The left expression in the comparison. */
    Expression leftExpr;

    /** The right expression in the comparison. */
    Expression rightExpr;



    public ArithmeticOperator(Type type, Expression lhs, Expression rhs) {
        if (type == null || lhs == null || rhs == null)
            throw new NullPointerException();

        leftExpr = lhs;
        rightExpr = rhs;

        this.type = type;
    }


    @Override
    public ColumnInfo getColumnInfo(Schema schema) throws SchemaNameException {
        ColumnInfo ltColInfo = leftExpr.getColumnInfo(schema);
        ColumnInfo rtColInfo = rightExpr.getColumnInfo(schema);

        SQLDataType resultSQLType = getSQLResultType(
            ltColInfo.getType().getBaseType(), rtColInfo.getType().getBaseType());

        ColumnType colType = new ColumnType(resultSQLType);
        return new ColumnInfo(toString(), colType);
    }


    private SQLDataType getSQLResultType(SQLDataType lType, SQLDataType rType) {
        // In case of division, we always return a double-precision result.
        if (type == Type.DIVIDE)
            return SQLDataType.DOUBLE;

        // This array specifies the type-conversion sequence.  If at least one of
        // the arguments is type typeOrder[i], then both arguments are coerced to
        // that type.  (This is not entirely accurate at the moment, but is
        // perfectly sufficient for our needs.)
        SQLDataType[] typeOrder = {
            SQLDataType.NUMERIC, SQLDataType.DOUBLE, SQLDataType.FLOAT,
            SQLDataType.BIGINT, SQLDataType.INTEGER, SQLDataType.SMALLINT,
            SQLDataType.TINYINT
        };

        for (SQLDataType aTypeOrder : typeOrder) {
            if (lType == aTypeOrder || rType == aTypeOrder)
                return aTypeOrder;
        }

        // Just guess INTEGER.  Works for C...
        return SQLDataType.INTEGER;
    }


    public Object evaluate(Environment env) throws ExpressionException {
        // Evaluate the left and right subexpressions.
        Object lhsValue = leftExpr.evaluate(env);
        Object rhsValue = rightExpr.evaluate(env);

        // If either the LHS value or RHS value is NULL (represented by Java
        // null value) then the entire expression evaluates to NULL.
        if (lhsValue == null || rhsValue == null)
            return null;

        return evalObjects(type, lhsValue, rhsValue);
    }


    /**
     * This static helper method can be used to compute basic arithmetic
     * operations between two arguments.  It is of course used to evaluate
     * <tt>ArithmeticOperator</tt> objects, but it can also be used to evaluate
     * specific arithmetic operations within other components of the database
     * system.
     *
     * @param type the arithmetic operation to perform
     * @param aObj the first operand value for the operation
     * @param bObj the second operand value for the operation
     *
     * @return the result of the arithmetic operation
     *
     * @throws ExpressionException if the operand type is unrecognized
     */
    public static Object evalObjects(Type type, Object aObj, Object bObj) {
        // Coerce the values to both have the same numeric type.

        TypeConverter.Pair coerced = TypeConverter.coerceArithmetic(aObj, bObj);

        Object result;

        if (coerced.value1 instanceof Double) {
            result = evalDoubles(type, (Double) coerced.value1, (Double) coerced.value2);
        }
        else if (coerced.value1 instanceof Float) {
            result = evalFloats(type, (Float) coerced.value1, (Float) coerced.value2);
        }
        else if (coerced.value1 instanceof Long) {
            result = evalLongs(type, (Long) coerced.value1, (Long) coerced.value2);
        }
        else {
            assert coerced.value1 instanceof Integer;
            result = evalIntegers(type, (Integer) coerced.value1, (Integer) coerced.value2);
        }

        return result;
    }


    /**
     * This helper implements the arithmetic operations for <tt>Double</tt>
     * values.  Note that division of two <tt>Double</tt>s will produce a
     * <tt>Double</tt>.
     *
     * @param type the arithmetic operation to perform
     * @param aObj the first operand value for the operation
     * @param bObj the second operand value for the operation
     *
     * @return the result of the arithmetic operation
     *
     * @throws ExpressionException if the operand type is unrecognized
     */
    private static Double evalDoubles(Type type, Double aObj, Double bObj) {
        double a = aObj.doubleValue();
        double b = bObj.doubleValue();
        double result;

        switch (type) {
        case ADD:
            result = a + b;
            break;

        case SUBTRACT:
            result = a - b;
            break;

        case MULTIPLY:
            result = a * b;
            break;

        case DIVIDE:
            // TODO:  How to handle divide-by-zero?
            result = a / b;
            break;

        case REMAINDER:
            result = a % b;
            break;

        case POWER:
            // TODO:  How to handle 0^0?
            result = Math.pow(a, b);
            break;

        default:
            throw new ExpressionException("Unrecognized arithmetic type " + type);
        }

        return Double.valueOf(result);
    }


    /**
     * This helper implements the arithmetic operations for <tt>Float</tt>
     * values.  Note that division of two <tt>Float</tt>s will produce a
     * <tt>Float</tt>.
     *
     * @param type the arithmetic operation to perform
     * @param aObj the first operand value for the operation
     * @param bObj the second operand value for the operation
     *
     * @return the result of the arithmetic operation
     *
     * @throws ExpressionException if the operand type is unrecognized
     */
    private static Float evalFloats(Type type, Float aObj, Float bObj) {
        float a = aObj.floatValue();
        float b = bObj.floatValue();
        float result;

        switch (type) {
        case ADD:
            result = a + b;
            break;

        case SUBTRACT:
            result = a - b;
            break;

        case MULTIPLY:
            result = a * b;
            break;

        case DIVIDE:
            // TODO:  How to handle divide-by-zero?
            result = a / b;
            break;

        case REMAINDER:
            result = a % b;
            break;

        case POWER:
            // TODO:  How to handle 0^0?
            result = (float)Math.pow(a, b);
            break;

        default:
            throw new ExpressionException("Unrecognized arithmetic type " + type);
        }

        return Float.valueOf(result);
    }


    /**
     * This helper implements the arithmetic operations for <tt>Long</tt>
     * values.  Note that division of two <tt>Long</tt>s will produce a
     * <tt>Double</tt>, not a <tt>Long</tt>.
     *
     * @param type the arithmetic operation to perform
     * @param aObj the first operand value for the operation
     * @param bObj the second operand value for the operation
     *
     * @return the result of the arithmetic operation
     *
     * @throws ExpressionException if the operand type is unrecognized
     */
    private static Object evalLongs(Type type, Long aObj, Long bObj) {
        long a = aObj.longValue();
        long b = bObj.longValue();

        if (type == Type.DIVIDE) {
            // TODO:  How to handle divide-by-zero?
            double result = (double) a / (double) b;
            return new Double(result);
        }

        long result;

        switch (type) {
        case ADD:
            result = a + b;
            break;

        case SUBTRACT:
            result = a - b;
            break;

        case MULTIPLY:
            result = a * b;
            break;

        // Division is handled separately.

        case REMAINDER:
            result = a % b;
            break;

        case POWER:
            // TODO:  How to handle 0^0?
            result = (long)Math.pow(a, b);
            break;

        default:
            throw new ExpressionException("Unrecognized arithmetic type " + type);
        }

        return Long.valueOf(result);
    }


    /**
     * This helper implements the arithmetic operations for <tt>Integer</tt>
     * values.  Note that division of two <tt>Integer</tt>s will produce a
     * <tt>Double</tt>, not an <tt>Integer</tt>.
     *
     * @param type the arithmetic operation to perform
     * @param aObj the first operand value for the operation
     * @param bObj the second operand value for the operation
     *
     * @return the result of the arithmetic operation
     *
     * @throws ExpressionException if the operand type is unrecognized
     */
    private static Object evalIntegers(Type type, Integer aObj, Integer bObj) {
        int a = aObj.intValue();
        int b = bObj.intValue();

        if (type == Type.DIVIDE) {
            // TODO:  How to handle divide-by-zero?
            double result = (double) a / (double) b;
            return new Double(result);
        }

        int result;

        switch (type) {
        case ADD:
            result = a + b;
            break;

        case SUBTRACT:
            result = a - b;
            break;

        case MULTIPLY:
            result = a * b;
            break;

        // Division is handled separately.

        case REMAINDER:
            result = a % b;
            break;

        case POWER:
            // TODO:  How to handle 0^0?
            result = (int) Math.pow(a, b);
            break;

        default:
            throw new ExpressionException("Unrecognized arithmetic type " + type);
        }

        return Integer.valueOf(result);
    }


    @Override
    public Expression traverse(ExpressionProcessor p) {
        p.enter(this);
        leftExpr = leftExpr.traverse(p);
        rightExpr = rightExpr.traverse(p);
        return p.leave(this);
    }


    /**
     * Returns a string representation of this arithmetic expression and its
     * subexpressions, including parentheses where necessary to specify
     * precedence.
     */
    @Override
    public String toString() {
        // Convert all of the components into string representations.
        String leftStr = leftExpr.toString();
        String rightStr = rightExpr.toString();
        String opStr = " " + type.stringRep() + " ";

        // Figure out if I need parentheses around the subexpressions.

        if (type == Type.MULTIPLY || type == Type.DIVIDE || type == Type.REMAINDER) {
            if (leftExpr instanceof ArithmeticOperator) {
                ArithmeticOperator leftOp = (ArithmeticOperator) leftExpr;
                if (leftOp.type == Type.ADD || leftOp.type == Type.SUBTRACT)
                    leftStr = "(" + leftStr + ")";
            }

            if (rightExpr instanceof ArithmeticOperator) {
                ArithmeticOperator rightOp = (ArithmeticOperator) rightExpr;
                if (rightOp.type == Type.ADD || rightOp.type == Type.SUBTRACT)
                    rightStr = "(" + rightStr + ")";
            }
        }

        return leftStr + opStr + rightStr;
    }


    /**
     * Simplifies an arithmetic expression, computing as much of the expression
     * as possible.
     */
    public Expression simplify() {
        leftExpr = leftExpr.simplify();
        rightExpr = rightExpr.simplify();

        if (!leftExpr.hasSymbols())
            leftExpr = new LiteralValue(leftExpr.evaluate());

        if (!rightExpr.hasSymbols())
            rightExpr = new LiteralValue(rightExpr.evaluate());

        if (!hasSymbols())
            return new LiteralValue(evaluate());

        return this;
    }


    /**
     * Checks if the argument is an expression with the same structure, but not
     * necesarily the same references.
     *
     * @param obj the object to which we are comparing
     */
    @Override
    public boolean equals(Object obj) {
        if (obj instanceof ArithmeticOperator) {
            ArithmeticOperator other = (ArithmeticOperator) obj;
            return (type.equals(other.type) &&
                    leftExpr.equals(other.leftExpr) &&
                    rightExpr.equals(other.rightExpr));
        }
        return false;
    }


    /**
     * Computes the hashcode of an Expression.  This method is used to see if
     * two expressions CAN be equal.
     */
    @Override
    public int hashCode() {
        int hash = 7;

        hash = 31 * hash + type.hashCode();

        hash = 31 * hash + leftExpr.hashCode();
        hash = 31 * hash + rightExpr.hashCode();

        return hash;
    }


    /** Creates a copy of expression. */
    @Override
    protected Object clone() throws CloneNotSupportedException {
        ArithmeticOperator expr = (ArithmeticOperator)super.clone();

        // Type is immutable, copy it.
        expr.type = this.type;

        // Clone the subexpressions
        expr.leftExpr = (Expression) leftExpr.clone();
        expr.rightExpr = (Expression) rightExpr.clone();

        return expr;
    }
}
