package edu.caltech.nanodb.server.properties;


/**
 * This exception is thrown when an attempt is made to read or write to a
 * nonexistent property.
 */
public class UnrecognizedPropertyException extends Exception {

    public UnrecognizedPropertyException() {
        super();
    }


    public UnrecognizedPropertyException(String msg) {
        super(msg);
    }


    public UnrecognizedPropertyException(Throwable cause) {
        super(cause);
    }


    public UnrecognizedPropertyException(String msg, Throwable cause) {
        super(msg, cause);
    }
}
