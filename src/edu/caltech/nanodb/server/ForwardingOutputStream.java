package edu.caltech.nanodb.server;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;


/**
 * Created by donnie on 10/26/14.
 */
public class ForwardingOutputStream extends ByteArrayOutputStream {

    private ObjectOutputStream objectOutput;


    public ForwardingOutputStream(ObjectOutputStream objectOutput) {
        this.objectOutput = objectOutput;
    }


    public void flush() throws IOException {
        String contents = toString();
        objectOutput.writeObject(contents);
        objectOutput.flush();
        reset();
    }
}
