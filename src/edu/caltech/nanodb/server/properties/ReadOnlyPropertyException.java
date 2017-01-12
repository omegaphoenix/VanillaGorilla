package edu.caltech.nanodb.server.properties;


/**
 * This exception is thrown when an attempt is made to write to a read-only
 * property.
 */
public class ReadOnlyPropertyException extends Exception {

    public ReadOnlyPropertyException() {
        super();
    }


    public ReadOnlyPropertyException(String msg) {
        super(msg);
    }


    public ReadOnlyPropertyException(Throwable cause) {
        super(cause);
    }


    public ReadOnlyPropertyException(String msg, Throwable cause) {
        super(msg, cause);
    }
}
