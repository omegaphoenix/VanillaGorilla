package edu.caltech.nanodb.storage;


/**
 * This class and its subclasses represent general storage errors that can be
 * encountered when manipulating the various data files that comprise the
 * database.
 */
public class StorageException extends Exception {

    /** Construct a storage exception with no message. */
    public StorageException() {
        super();
    }

    /**
     * Construct a storage exception with the specified message.
     */
    public StorageException(String msg) {
        super(msg);
    }


    /**
     * Construct a storage exception with the specified cause and no message.
     */
    public StorageException(Throwable cause) {
        super(cause);
    }


    /**
     * Construct a storage exception with the specified message and cause.
     */
    public StorageException(String msg, Throwable cause) {
        super(msg, cause);
    }
}

