package edu.caltech.nanodb.expressions;


import edu.caltech.nanodb.relations.ColumnInfo;
import edu.caltech.nanodb.relations.ColumnType;
import edu.caltech.nanodb.relations.Schema;
import edu.caltech.nanodb.relations.SchemaNameException;


public class IsNullOperator extends Expression {

    /** The expression to evaluate for "nullness". */
    private Expression expr;

    /**
     * If this is false, the operator computes <tt>IS NULL</tt>; if true,
     * the operator computes <tt>IS NOT NULL</tt>.
     */
    private boolean invert;


    public IsNullOperator(Expression expr, boolean invert) {
        this.expr = expr;
        this.invert = invert;
    }


    @Override
    public ColumnInfo getColumnInfo(Schema schema) throws SchemaNameException {
        // This operator always returns Boolean values, so just pass a Boolean
        // value in to the TypeConverter to get out the corresponding SQL
        // datatype.
        ColumnType colType =
            new ColumnType(TypeConverter.getSQLType(Boolean.FALSE));

        return new ColumnInfo(colType);
    }


    @Override
    public Object evaluate(Environment env) throws ExpressionException {
        Object exprResult = expr.evaluate(env);

        // result = (exprResult IS NULL)
        boolean result = (exprResult == null);
        if (invert)
            result = !result;  // result = (exprResult IS NOT NULL)

        return Boolean.valueOf(result);
    }


    /**
     * Simplifies this expression, computing as much of the expression as
     * possible.
     */
    public Expression simplify() {
        expr = expr.simplify();
        if (!expr.hasSymbols()) {
            expr = new LiteralValue(expr.evaluate());
            return new LiteralValue(evaluate());
        }

        return this;
    }



    @Override
    public Expression traverse(ExpressionProcessor p) {
        p.enter(this);
        expr = expr.traverse(p);
        return p.leave(this);
    }


    @Override
    public boolean equals(Object obj) {
        if (obj instanceof IsNullOperator) {
            IsNullOperator other = (IsNullOperator) obj;
            return (expr.equals(other.expr) && invert == other.invert);
        }
        return false;
    }

    @Override
    public int hashCode() {
        int hash = 37;
        hash = 43 * hash + expr.hashCode();
        hash = 43 * hash + (invert ? 1 : 0);
        return hash;
    }

    @Override
    public String toString() {
        return expr.toString() + (invert ? " IS NOT NULL" : " IS NULL");
    }


    /** Creates a copy of expression. */
    @Override
    protected Object clone() throws CloneNotSupportedException {
        IsNullOperator dup = (IsNullOperator) super.clone();

        // Clone the subexpression
        dup.expr = (Expression) expr.clone();

        return dup;
    }
}
