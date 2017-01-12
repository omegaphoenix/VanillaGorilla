package edu.caltech.nanodb.expressions;


/**
 * This exception is a specific subtype of the type-cast exception that is
 * thrown when a type-conversion would cause a truncation of a value.
 */
public class TruncationException extends TypeCastException {

    /** Create a new <tt>TruncationException</tt> with no message or cause. */
    public TruncationException() {
        super();
    }


    /**
     * Create a new <tt>TruncationException</tt> with the specified message.
     */
    public TruncationException(String message) {
        super(message);
    }


    /**
     * Create a new <tt>TruncationException</tt> with the specified cause.
     */
    public TruncationException(Throwable cause) {
        super(cause);
    }


    /**
     * Create a new <tt>TruncationException</tt> with the specified message
     * and cause.
     */
    public TruncationException(String message, Throwable cause) {
        super(message, cause);
    }
}
