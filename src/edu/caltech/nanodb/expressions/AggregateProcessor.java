package edu.caltech.nanodb.expressions;

import java.io.IOException;

import java.util.Map;
import java.util.HashMap;

import edu.caltech.nanodb.functions.ScalarFunction;
import edu.caltech.nanodb.functions.AggregateFunction;
import edu.caltech.nanodb.expressions.ColumnValue;


public class AggregateProcessor implements ExpressionProcessor {

    private Map<String, FunctionCall> aggregates;
    private boolean currentExprHasAggr;

    public AggregateProcessor() {
        aggregates = new HashMap<>();
    }

    public void enter(Expression e) {
        currentExprHasAggr = false;
        return;
    }

    public Expression leave(Expression e) throws IOException {
        if (e instanceof FunctionCall) {
            FunctionCall call = (FunctionCall) e;
            ScalarFunction f = call.getFunction();
            if (f instanceof AggregateFunction) {
                if (currentExprHasAggr) {
                    throw new IOException("Aggregate function contains another aggregate function");
                }
                currentExprHasAggr = true;
                int count = aggregates.size();
                String uuid = String.format("#A%d", count);
                ColumnValue renamed = new ColumnValue(new ColumnName(uuid));
                aggregates.put(uuid, call);
                return renamed;
            }
        }
        return e;
    }

    public boolean currentHasAggregate() {
        return currentExprHasAggr;
    }

    public Map<String, FunctionCall> getAllAggregates() {
        return aggregates;
    }
}
