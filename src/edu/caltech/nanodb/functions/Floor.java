package edu.caltech.nanodb.functions;


import java.util.List;

import edu.caltech.nanodb.expressions.Environment;
import edu.caltech.nanodb.expressions.Expression;
import edu.caltech.nanodb.expressions.ExpressionException;
import edu.caltech.nanodb.expressions.TypeConverter;

import edu.caltech.nanodb.relations.ColumnType;
import edu.caltech.nanodb.relations.SQLDataType;
import edu.caltech.nanodb.relations.Schema;


/**
 * Computes the largest whole number smaller than the argument.  Returns NULL if
 * argument is NULL.  The result is always a double-precision number, even
 * though it is a whole number, since this is what {@link Math#floor} returns.
 */
public class Floor extends SimpleFunction {
    @Override
    public ColumnType getReturnType(List<Expression> args, Schema schema) {
        // We could try to return an int or long, but Math.floor() always
        // returns a double, so we'll just return a double too.
        return new ColumnType(SQLDataType.DOUBLE);
    }

    @Override
    public Object evaluate(Environment env, List<Expression> args) {
        if (args.size() != 1) {
            throw new ExpressionException("Cannot call CEIL on " + args.size()
                + " arguments");
        }

        Object argVal = args.get(0).evaluate(env);

        if (argVal == null)
            return null;

        return Math.floor(TypeConverter.getDoubleValue(argVal));
    }
}
