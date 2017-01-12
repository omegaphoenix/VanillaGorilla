package edu.caltech.nanodb.functions;


import java.util.List;

import edu.caltech.nanodb.expressions.Environment;
import edu.caltech.nanodb.expressions.Expression;
import edu.caltech.nanodb.expressions.ExpressionException;

import edu.caltech.nanodb.relations.ColumnType;
import edu.caltech.nanodb.relations.Schema;


/**
 * Returns the least value of all arguments.  <tt>NULL</tt> arguments are
 * ignored; the function only produces <tt>NULL</tt> if all inputs are
 * <tt>NULL</tt>.  The type of the result is the type of the first argument.
 */
public class Least extends SimpleFunction {
    @Override
    public ColumnType getReturnType(List<Expression> args, Schema schema) {
        if (args.size() < 2) {
            throw new ExpressionException("Cannot call GREATEST on " +
                args.size() + " arguments");
        }

        // Return the type of the first argument.
        return args.get(0).getColumnInfo(schema).getType();
    }


    @Override
    @SuppressWarnings("unchecked")
    public Object evaluate(Environment env, List<Expression> args) {
        if (args.size() < 2) {
            throw new ExpressionException("Cannot call GREATEST on " +
                args.size() + " arguments");
        }

        Comparable greatest = null;
        for (Expression arg : args) {
            Comparable val = (Comparable) arg.evaluate(env);
            if (val == null)
                continue;

            if (greatest == null || val.compareTo(greatest) < 0)
                greatest = val;
        }

        return greatest;
    }
}
