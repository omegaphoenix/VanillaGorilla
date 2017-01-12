package edu.caltech.nanodb.functions;


/**
 * Created by donnie on 12/7/13.
 */
public class CountDistinct extends CountAggregate {

    public CountDistinct() {
        super(/* distinct */ true, /* sortedInputs */ false);
    }
}
