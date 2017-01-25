package edu.caltech.nanodb.expressions;

import edu.caltech.nanodb.functions.ScalarFunction;
import edu.caltech.nanodb.functions.AggregateFunction;


public class AggregateProcessor implements ExpressionProcessor {

    // TODO: map

    public void enter(Expression e) {
        if (e instanceof FunctionCall) {
            FunctionCall call = (FunctionCall) e;
            ScalarFunction f = call.getFunction();
            if (f instanceof AggregateFunction) {
                // Rename?

            }
        }
    }

    public Expression leave(Expression e) {
        // TODO: return renamed expression?
        return e;
    }

}
