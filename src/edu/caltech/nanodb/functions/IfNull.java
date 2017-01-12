package edu.caltech.nanodb.functions;


import java.util.List;

import edu.caltech.nanodb.expressions.Environment;
import edu.caltech.nanodb.expressions.Expression;
import edu.caltech.nanodb.expressions.ExpressionException;

import edu.caltech.nanodb.relations.ColumnType;
import edu.caltech.nanodb.relations.Schema;


/**
 * Implements {@code IFNULL(expr1, expr2)}. If the first argument is
 * {@code NULL}, returns the second argument, else returns the first argument.
 * 
 * @author emil
 */
public class IfNull extends SimpleFunction {
    @Override
    public ColumnType getReturnType(List<Expression> args, Schema schema) {
        if (args.size() != 2) {
            throw new ExpressionException("Cannot call IFNULL on " +
                args.size() + " arguments");
        }

        // Return the type of the first argument.
        return args.get(0).getColumnInfo(schema).getType();
    }


    @Override
    public Object evaluate(Environment env, List<Expression> args) {
        if (args.size() != 2) {
            throw new ExpressionException("Cannot call IFNULL on " +
                args.size() + " arguments");
        }
        
        Object argVal1 = args.get(0).evaluate(env);
        Object argVal2 = args.get(1).evaluate(env);
        
        if (argVal1 == null)
            return argVal2;
        else
            return argVal1;
    }

}
