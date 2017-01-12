package edu.caltech.nanodb.server;


import java.io.IOException;
import java.io.ObjectOutputStream;

import edu.caltech.nanodb.expressions.TupleLiteral;
import edu.caltech.nanodb.queryeval.TupleProcessor;
import edu.caltech.nanodb.relations.Schema;
import edu.caltech.nanodb.relations.Tuple;


/**
 * This implementation of the tuple-processor interface sends the schema and
 * tuples produced by the <tt>SELECT</tt> statement over an
 * {@link ObjectOutputStream}.
 */
public class TupleSender implements TupleProcessor {

    private ObjectOutputStream objectOutput;


    public TupleSender(ObjectOutputStream objectOutput) {
        if (objectOutput == null)
            throw new IllegalArgumentException("objectOutput cannot be null");

        this.objectOutput = objectOutput;
    }


    public void setSchema(Schema schema) throws IOException {
        // If the incoming schema-object is not of type Schema then make a copy
        // of it and send it over.
        if (!schema.getClass().equals(Schema.class))
            schema = new Schema(schema);

        objectOutput.writeObject(schema);
    }


    public void process(Tuple tuple) throws IOException {
        TupleLiteral tupLit;

        if (!(tuple instanceof TupleLiteral))
            tupLit = new TupleLiteral(tuple);
        else
            tupLit = (TupleLiteral) tuple;

        objectOutput.writeObject(tupLit);
    }


    public void finish() {
        // Not used
    }
}
