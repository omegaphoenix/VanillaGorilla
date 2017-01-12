package edu.caltech.nanodb.storage.writeahead;


import java.io.IOException;


/**
 * This exception class represents issues with write-ahead log files, when they
 * contain corrupt or invalid information in some way.
 */
public class WALFileException extends IOException {
    public WALFileException() {
        super();
    }
    
    
    public WALFileException(String msg) {
        super(msg);
    }
    
    
    public WALFileException(Throwable cause) {
        super(cause);
    }
    
    
    public WALFileException(String msg, Throwable cause) {
        super(msg, cause);
    }
}
