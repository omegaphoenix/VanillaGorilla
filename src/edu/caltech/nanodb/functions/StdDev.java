package edu.caltech.nanodb.functions;


/**
 * Created by donnie on 12/7/13.
 */
public class StdDev extends StdDevVarAggregate {
    public StdDev() {
        super(/* computeStdDev */ true);
    }
}
