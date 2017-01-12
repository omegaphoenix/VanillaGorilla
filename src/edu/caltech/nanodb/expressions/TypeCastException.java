package edu.caltech.nanodb.expressions;


/**
 * This exception is thrown when the {@link TypeConverter} tries to cast
 * a value from one type to another incompatible type.
 */
public class TypeCastException extends RuntimeException {

    /** Create a new <code>TypeCastException</code> with no message or cause. */
    public TypeCastException() {
        super();
    }


    /**
     * Create a new <code>TypeCastException</code> with the specified message.
     */
    public TypeCastException(String message) {
        super(message);
    }


    /**
     * Create a new <code>TypeCastException</code> with the specified cause.
     */
    public TypeCastException(Throwable cause) {
        super(cause);
    }


    /**
     * Create a new <code>TypeCastException</code> with the specified message
     * and cause.
     */
    public TypeCastException(String message, Throwable cause) {
        super(message, cause);
    }
}
