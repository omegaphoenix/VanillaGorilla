package edu.caltech.nanodb.expressions;

import java.io.IOException;

import java.util.Map;
import java.util.HashMap;

import edu.caltech.nanodb.functions.ScalarFunction;
import edu.caltech.nanodb.functions.AggregateFunction;
import edu.caltech.nanodb.expressions.ColumnValue;


public class AggregateProcessor implements ExpressionProcessor {

    public Map<String, FunctionCall> aggregates;
    public boolean currentExprHasAggr;
    private boolean aggregatesAllowed;

    public AggregateProcessor(boolean aggregatesAllowed) {
        this.aggregatesAllowed = aggregatesAllowed;
        if (aggregatesAllowed) {
            aggregates = new HashMap<>();
        }
    }

    public void enter(Expression e) throws IllegalArgumentException {
        if (e instanceof FunctionCall) {
            FunctionCall call = (FunctionCall) e;
            ScalarFunction f = call.getFunction();
            if (f instanceof AggregateFunction) {
                if (!aggregatesAllowed) {
                    throw new IllegalArgumentException("Aggregate function not allowed in expression");
                }
                if (currentExprHasAggr) {
                    throw new IllegalArgumentException("Aggregate function in aggregate function");
                }
                currentExprHasAggr = true;
            }
        }
        return;
    }

    public Expression leave(Expression e) throws IllegalArgumentException{
        if (e instanceof FunctionCall) {
            FunctionCall call = (FunctionCall) e;
            ScalarFunction f = call.getFunction();
            if (f instanceof AggregateFunction) {
                currentExprHasAggr = false;
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
