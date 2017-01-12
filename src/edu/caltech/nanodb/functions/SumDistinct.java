package edu.caltech.nanodb.functions;


/**
 * Created by donnie on 12/7/13.
 */
public class SumDistinct extends SumAvgAggregate {
    public SumDistinct() {
        super(/* computeAverage */ false, /* distinct */ true);
    }
}
