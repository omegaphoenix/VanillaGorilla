package edu.caltech.nanodb.commands;


/**
 * This exception is thrown when a fatal error occurs during command
 * execution.
 */
public class ExecutionException extends Exception {

    public ExecutionException() {
        super();
    }


    public ExecutionException(String msg) {
        super(msg);
    }


    public ExecutionException(Throwable cause) {
        super(cause);
    }


    public ExecutionException(String msg, Throwable cause) {
        super(msg, cause);
    }
}


