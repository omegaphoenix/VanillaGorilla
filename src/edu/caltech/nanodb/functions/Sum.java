package edu.caltech.nanodb.functions;


/**
 * Created by donnie on 12/7/13.
 */
public class Sum extends SumAvgAggregate {
    public Sum() {
        super(/* computeAverage */ false, /* distinct */ false);
    }
}
