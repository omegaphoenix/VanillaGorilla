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
 * Computes the sine of a single argument. Returns  NULL if argument
 * is NULL.
 * 
 * @author emil
 */
public class Sin extends SimpleFunction {
    @Override
    public ColumnType getReturnType(List<Expression> args, Schema schema) {
        return new ColumnType(SQLDataType.DOUBLE);
    }


    @Override
    public Object evaluate(Environment env, List<Expression> args) {
        if (args.size() != 1) {
            throw new ExpressionException("Cannot call SIN on " + args.size() +
                " arguments");
        }

        Object argVal = args.get(0).evaluate(env);
        
        if (argVal == null)
            return null;

        return Math.sin(TypeConverter.getDoubleValue(argVal));
    }
}
