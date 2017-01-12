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
 * Computes the absolute value of a single argument. Returns NULL if argument
 * is NULL.
 * 
 * @author emil
 */
public class Abs extends SimpleFunction {

    @Override
    public ColumnType getReturnType(List<Expression> args, Schema schema) {
        if (args.size() < 1) {
            throw new ExpressionException("Cannot call ABS on " + args.size() +
                " arguments");
        }

        // Return the type of the first argument.
        ColumnType argType = args.get(0).getColumnInfo(schema).getType();
        SQLDataType argBaseType = argType.getBaseType();

        if (SQLDataType.isNumber(argBaseType))
            return argType;

        return new ColumnType(SQLDataType.DOUBLE);
    }


    @Override
    public Object evaluate(Environment env, List<Expression> args) {
        if (args.size() != 1) {
            throw new ExpressionException("Cannot call ABS on " + args.size() +
                " arguments");
        }
        
        Object argVal = args.get(0).evaluate(env);

        // A NULL input results in a NULL output.
        if (argVal == null)
            return null;

        if (argVal instanceof Byte) {
            return (byte) Math.abs((Byte) argVal);
        }
        else if (argVal instanceof Short) {
            return (short) Math.abs((Short) argVal);
        }
        else if (argVal instanceof Integer) {
            return Math.abs((Integer) argVal);
        }
        else if (argVal instanceof Long) {
            return Math.abs((Long) argVal);
        }
        else if (argVal instanceof Float) {
            return Math.abs((Float) argVal);
        }

        // If we got here then the argument wasn't any of the standard numeric
        // types.  Try to cast the input to a double-precision value!
        return Math.abs(TypeConverter.getDoubleValue(argVal));
    }
}
