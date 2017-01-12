package edu.caltech.nanodb.functions;


/**
 * Created by donnie on 12/6/13.
 */
public class Count extends CountAggregate {
    public Count() {
        super(/* distinct */ false, /* sortedInputs */ false);
    }
}
