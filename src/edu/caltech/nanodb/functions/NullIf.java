package edu.caltech.nanodb.functions;


import java.util.List;

import edu.caltech.nanodb.expressions.Environment;
import edu.caltech.nanodb.expressions.Expression;
import edu.caltech.nanodb.expressions.ExpressionException;
import edu.caltech.nanodb.expressions.TypeConverter;

import edu.caltech.nanodb.relations.ColumnType;
import edu.caltech.nanodb.relations.Schema;


/**
 * Implements {@code NULLIF(cond, expr)}. Returns {@code NULL} if the first
 * argument is {@code TRUE}, else returns the second argument.
 * 
 * @author emil
 */
public class NullIf extends SimpleFunction {
    @Override
    public ColumnType getReturnType(List<Expression> args, Schema schema) {
        if (args.size() != 2) {
            throw new ExpressionException("Cannot call NULLIF on " +
                args.size() + " arguments");
        }

        // Return the type of the second argument.
        return args.get(1).getColumnInfo(schema).getType();
    }


    @Override
    public Object evaluate(Environment env, List<Expression> args) {
        if (args.size() != 2) {
            throw new ExpressionException("Cannot call NULLIF on " + args.size() 
                    + " arguments");
        }

        Object condVal = args.get(0).evaluate(env);

        if (condVal != null && TypeConverter.getBooleanValue(condVal))
            return null;
        else
            return args.get(1).evaluate(env);
    }

}
