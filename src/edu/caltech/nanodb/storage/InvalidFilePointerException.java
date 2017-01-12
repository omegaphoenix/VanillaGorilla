package edu.caltech.nanodb.storage;


/**
 * This exception class can be thrown when a file-pointer is discovered to be
 * invalid for some reason.  For example, file-pointers can be invalid if they
 * do not point to an actual tuple in a table-file.
 */
public class InvalidFilePointerException extends StorageException {

    /** Construct an invalid file-pointer exception with no message. */
    public InvalidFilePointerException() {
        super();
    }

    /**
     * Construct an invalid file-pointer exception with the specified message.
     */
    public InvalidFilePointerException(String msg) {
        super(msg);
    }


    /**
     * Construct an invalid file-pointer exception with the specified cause
     * and no message.
     */
    public InvalidFilePointerException(Throwable cause) {
        super(cause);
    }


    /**
     * Construct an invalid file-pointer exception with the specified message
     * and cause.
     */
    public InvalidFilePointerException(String msg, Throwable cause) {
        super(msg, cause);
    }
}
