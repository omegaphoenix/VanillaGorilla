package edu.caltech.nanodb.client;


import java.io.PrintStream;
import java.util.concurrent.atomic.AtomicInteger;

import edu.caltech.nanodb.transactions.TransactionState;


/**
 * This class holds all session state associated with a particular client
 * accessing the database.  This object can be stored in thread-local storage
 * to ensure that it can be accessed throughout the database engine to determine
 * current client settings.
 */
public class SessionState {

    /*========================================================================
     * STATIC FIELDS AND METHODS
     */


    /**
     * This static variable holds the next session ID to assign to a client
     * session.  It must be accessed in a synchronized manner.
     */
    static private AtomicInteger nextSessionID = new AtomicInteger(1);

    
    private static ThreadLocal<SessionState> threadLocalState =
        new ThreadLocal<SessionState>() {
            @Override protected SessionState initialValue() {
                return new SessionState(nextSessionID.getAndIncrement());
            }
        };


    /**
     * Returns the current session state, possibly initializing a new session
     * with its own unique ID in the process.  This value is stored in
     * thread-local storage, so no thread-safety is required when manipulating
     * the returned object.
     *
     * @return the session-state for this local thread.
     */
    public static SessionState get() {
        return threadLocalState.get();
    }


    /**
     * Removes the session-state from the thread's thread-local storage.
     */
    public static void remove() {
        threadLocalState.remove();
    }


    /*========================================================================
     * NON-STATIC FIELDS AND METHODS
     */


    /** The unique session ID assigned to this client session. */
    private int sessionID;


    /**
     * This is the output stream for the current client.  If there are multiple
     * clients, writing to this stream will go to the client associated with
     * this session.
     */
    private PrintStream outputStream;


    /** The transaction state of this session. */
    private TransactionState txnState;


    private SessionState(int sessionID) {
        this.sessionID = sessionID;
        txnState = new TransactionState();

        // By default, we'll use the standard output stream for the session's
        // output stream, but this will be overridden when clients connect over
        // a socket.
        setOutputStream(System.out);
    }


    /**
     * Returns the unique session ID for this client.
     * 
     * @return the unique session ID for this client.
     */
    public int getSessionID() {
        return sessionID;
    }


    public PrintStream getOutputStream() {
        return outputStream;
    }
    
    
    public void setOutputStream(PrintStream out) {
        if (out == null)
            throw new IllegalArgumentException("out cannot be null");

        outputStream = out;
    }


    public TransactionState getTxnState() {
        return txnState;
    }


    @Override
    public int hashCode() {
        return sessionID;
    }
}
