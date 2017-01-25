package edu.caltech.nanodb.expressions;

import java.util.Map;
import java.util.HashMap;

import edu.caltech.nanodb.functions.ScalarFunction;
import edu.caltech.nanodb.functions.AggregateFunction;
import edu.caltech.nanodb.expressions.ColumnValue;

public class AggregateProcessor implements ExpressionProcessor {

    Map<String, FunctionCall> aggregates;

    public AggregateProcessor() {
        aggregates = new HashMap<>();
    }

    public void enter(Expression e) {
        return;
    }

    public Expression leave(Expression e) {
        if (e instanceof FunctionCall) {
            FunctionCall call = (FunctionCall) e;
            ScalarFunction f = call.getFunction();
            if (f instanceof AggregateFunction) {
                int count = aggregates.size();
                String uuid = String.format("#A%d", count);
                ColumnValue renamed = new ColumnValue(new ColumnName(uuid));
                aggregates.put(uuid, call);
                return renamed;
            }
        }
        return e;
    }
}
