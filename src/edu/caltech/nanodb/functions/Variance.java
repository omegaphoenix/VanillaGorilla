package edu.caltech.nanodb.functions;


/**
 * Created by donnie on 12/7/13.
 */
public class Variance extends StdDevVarAggregate {
    public Variance() {
        super(/* computeStdDev */ false);
    }
}
