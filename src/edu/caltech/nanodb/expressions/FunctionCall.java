package edu.caltech.nanodb.expressions;


import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import edu.caltech.nanodb.functions.AggregateFunction;
import edu.caltech.nanodb.functions.FunctionDirectory;
import edu.caltech.nanodb.functions.ScalarFunction;
import edu.caltech.nanodb.functions.SimpleFunction;

import edu.caltech.nanodb.relations.ColumnInfo;
import edu.caltech.nanodb.relations.ColumnType;
import edu.caltech.nanodb.relations.Schema;
import edu.caltech.nanodb.relations.SchemaNameException;


/**
 * This class represents all kinds of function-call expressions, including
 * simple function calls, aggregate function calls, and table-returning
 * function calls.  The set of available functions to call is stored in the
 * {@link FunctionDirectory}.
 */
public class FunctionCall extends Expression {
    /** The string name of the function as specified in the original SQL. */
    private String funcName;

    /**
     * A flag indicating whether the <tt>DISTINCT</tt> keyword was used in
     * the function invocation, e.g. <tt>COUNT(DISTINCT n)</tt>.  This flag is
     * only used in the context of aggregate functions; if it is set for other
     * kinds of functions, it is a semantic error.
     */
    private boolean distinct;

    /** The list of one or more arguments for the function call. */
    private ArrayList<Expression> args;


    /**
     * The actual function object that implements the function call.  This
     * may be either a simple function or an aggregate function.
     */
    private ScalarFunction function;


    public FunctionCall(String funcName, boolean distinct,
                        ArrayList<Expression> args) {

        if (funcName == null)
            throw new IllegalArgumentException("funcName cannot be null");

        if (args == null)
            throw new IllegalArgumentException("args cannot be null");

        this.funcName = funcName;
        this.distinct = distinct;
        this.args = args;

        function = (ScalarFunction) FunctionDirectory.getInstance().getFunction(funcName);

        if (!(function instanceof AggregateFunction) && distinct) {
            throw new IllegalArgumentException(
                "distinct cannot be specified with non-aggregate functions");
        }
    }


    public FunctionCall(String funcName, boolean distinct, Expression... args) {
        this(funcName, distinct, new ArrayList<>(Arrays.asList(args)));
    }


    public ScalarFunction getFunction() {
        return function;
    }


    public List<Expression> getArguments() {
        return Collections.unmodifiableList(args);
    }


    @Override
    public ColumnInfo getColumnInfo(Schema schema) throws SchemaNameException {
        // Generate a column-info object with the type indicated by the
        // function object, and with a column-name generated from the
        // expression.
        ColumnType colType = function.getReturnType(args, schema);
        return new ColumnInfo(toString(), colType);
    }


    @Override
    public Object evaluate(Environment env) throws ExpressionException {
        if (!(function instanceof SimpleFunction)) {
            throw new ExpressionException("Function must be a subclass of " +
                "SimpleFunction to work with evaluate().");
        }

        Object result = ((SimpleFunction) function).evaluate(env, args);

        // Check to see if we have invalid outputs. If so, return null instead
        // of a NaN. The Integer class is unable to detect NaN's and division
        // by 0 though...
        if (result instanceof Double && Double.isNaN((Double)result) ||
            result instanceof Float && Float.isNaN((Float)result))
            return null;

        return result;
    }


    @Override
    public Expression traverse(ExpressionProcessor p) {
        p.enter(this);

        for (int i = 0; i < args.size(); i++) {
            Expression e = args.get(i).traverse(p);
            args.set(i, e);
        }

        return p.leave(this);
    }


    @Override
    public boolean equals(Object obj) {
        if (obj instanceof FunctionCall) {
            FunctionCall other = (FunctionCall) obj;
            return (funcName.equals(other.funcName) && args.equals(other.args));
        }
        return false;
    }

    @Override
    public int hashCode() {
        int hash = 7;

        hash = hash * 31 + funcName.hashCode();
        hash = hash * 31 + args.hashCode();

        return hash;
    }

    @Override
    public String toString() {
        StringBuilder buf = new StringBuilder();

        buf.append(funcName).append('(');

        if (distinct)
            buf.append("DISTINCT ");

        boolean first = true;
        for (Expression arg : args) {
            if (first)
                first = false;
            else
                buf.append(", ");

            buf.append(arg.toString());
        }

        buf.append(')');

        return buf.toString();
    }


    /**
     * Creates a copy of expression.
     */
    @Override
    @SuppressWarnings("unchecked")
    protected Object clone() throws CloneNotSupportedException {
        FunctionCall expr = (FunctionCall) super.clone();

        expr.args = (ArrayList<Expression>) args.clone();
        expr.function = (ScalarFunction) function.clone();

        return expr;
    }
}
