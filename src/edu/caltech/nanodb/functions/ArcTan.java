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
 * Computes the arc-tangent of a single argument.  Returns <tt>NULL</tt> if
 * argument is <tt>NULL</tt>.
 * 
 * @author emil
 */
public class ArcTan extends SimpleFunction {
    @Override
    public ColumnType getReturnType(List<Expression> args, Schema schema) {
        return new ColumnType(SQLDataType.DOUBLE);
    }


    @Override
    public Object evaluate(Environment env, List<Expression> args) {
        if (args.size() != 1) {
            throw new ExpressionException("Cannot call ATAN on " + args.size() 
                    + " arguments");
        }

        Object argVal = args.get(0).evaluate(env);

        if (argVal == null)
            return null;

        return Math.atan(TypeConverter.getDoubleValue(argVal));
    }
}
