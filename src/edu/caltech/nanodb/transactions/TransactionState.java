package edu.caltech.nanodb.transactions;


import edu.caltech.nanodb.storage.writeahead.LogSequenceNumber;


/**
 * <p>
 * This class manages the transaction state associated with every client
 * session.  The transaction state for the current session can be retrieved
 * like this:
 * </p>
 * <pre>    TransactionState txnState = SessionState.get().getTxnState()</pre>
 * <p>
 * <b>The transaction state should generally <u>not</u> be managed directly!</b>
 * Rather, the operations provided by the {@link TransactionManager} should be
 * used.
 * </p>
 */
public class TransactionState {

    public static final int NO_TRANSACTION = -1;


    private int transactionID = NO_TRANSACTION;


    private boolean userStartedTxn = false;


    private boolean performedWrites = false;


    /**
     * This value indicates whether the current transaction has been logged
     * to the write-ahead log yet.  We don't write the "T<sub>i</sub>:  start"
     * record until the transaction performs its first write operation.
     */
    private boolean loggedTxnStart = false;


    private LogSequenceNumber lastLSN = null;


    public int getTransactionID() {
        return transactionID;
    }


    public void setTransactionID(int transactionID) {
        this.transactionID = transactionID;
    }


    public boolean getUserStartedTxn() {
        return userStartedTxn;
    }


    public void setUserStartedTxn(boolean b) {
        userStartedTxn = b;
    }


    public boolean hasPerformedWrites() {
        return performedWrites;
    }


    public void setPerformedWrites(boolean b) {
        performedWrites = b;
    }


    public boolean hasLoggedTxnStart() {
        return loggedTxnStart;
    }


    public void setLoggedTxnStart(boolean b) {
        loggedTxnStart = b;
    }


    public LogSequenceNumber getLastLSN() {
        return lastLSN;
    }


    public void setLastLSN(LogSequenceNumber lsn) {
        lastLSN = lsn;
    }


    public void clear() {
        transactionID = NO_TRANSACTION;
        lastLSN = null;
        userStartedTxn = false;
        performedWrites = false;
        loggedTxnStart = false;
    }


    public boolean isTxnInProgress() {
        return (transactionID != NO_TRANSACTION);
    }
    
    
    @Override
    public String toString() {
        StringBuilder buf = new StringBuilder();
        buf.append("TxnState[");
        
        if (transactionID == NO_TRANSACTION) {
            buf.append("no transaction");
        }
        else {
            buf.append(String.format(
                "txnID=%d, userStarted=%s, loggedStart=%s, lastLSN=%s",
                transactionID, userStartedTxn, loggedTxnStart, lastLSN));
        }
        buf.append(']');
        
        return buf.toString();
    }
}
