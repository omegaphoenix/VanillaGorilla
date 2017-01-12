package edu.caltech.nanodb.relations;


/**
 * This exception and its subclasses are used to signal when a database
 * constraint is violated.
 */
public class ConstraintViolationException extends RuntimeException {
    public ConstraintViolationException() {
        super();
    }

    public ConstraintViolationException(String msg) {
        super(msg);
    }
    
    public ConstraintViolationException(Throwable cause) {
        super(cause);
    }
    
    public ConstraintViolationException(String msg, Throwable cause) {
        super(msg, cause);
    }
}
