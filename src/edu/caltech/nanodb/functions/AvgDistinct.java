package edu.caltech.nanodb.functions;


/**
 * Created by donnie on 12/7/13.
 */
public class AvgDistinct extends SumAvgAggregate {
    public AvgDistinct() {
        super(/* computeAverage */ true, /* distinct */ true);
    }
}
