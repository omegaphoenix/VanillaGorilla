package edu.caltech.nanodb.functions;


import java.util.List;

import edu.caltech.nanodb.expressions.Expression;
import edu.caltech.nanodb.relations.ColumnType;
import edu.caltech.nanodb.relations.SQLDataType;
import edu.caltech.nanodb.relations.Schema;


/**
 * Created by donnie on 11/1/14.
 */
public class CountStar extends AggregateFunction {
    int count = 0;


    public CountStar() {
        super(/* supportsDistinct */ false);
    }


    @Override
    public void clearResult() {
        count = 0;
    }


    @Override
    public void addValue(Object value) {
        count++;
    }


    @Override
    public Object getResult() {
        return Integer.valueOf(count);
    }


    @Override
    public ColumnType getReturnType(List<Expression> args, Schema schema) {
        if (args.size() != 1) {
            throw new IllegalArgumentException(
                "Count(*) aggregate function takes 1 argument; got " +
                    args.size());
        }

        // When counting, the resulting aggregate column is always an integer
        return new ColumnType(SQLDataType.INTEGER);
    }
}
