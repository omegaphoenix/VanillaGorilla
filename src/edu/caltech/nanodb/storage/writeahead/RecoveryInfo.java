package edu.caltech.nanodb.storage.writeahead;


import java.util.HashMap;


/**
 * This class holds information necessary for the {@link WALManager} to perform
 * recovery processing, such as the LSNs to scan for redo/undo processing, and
 * the set of incomplete transactions.
 */
public class RecoveryInfo {
    /** This is the log sequence number to start recovery processing from. */
    public LogSequenceNumber firstLSN;


    /**
     * This is the "next LSN", one past the last valid log sequence number
     * found in the write-ahead logs.
     */
    public LogSequenceNumber nextLSN;


    /**
     * This is the maximum transaction ID seen in the write-ahead logs.
     * The next transaction ID used by the database system will be one more
     * than this value.
     */
    public int maxTransactionID;

    /**
     * This is the set of incomplete transactions found during recovery
     * processing, along with the last log sequence number seen for each
     * transaction.
     */
    public HashMap<Integer, LogSequenceNumber> incompleteTxns;


    public RecoveryInfo(LogSequenceNumber firstLSN,
                        LogSequenceNumber nextLSN) {

        this.firstLSN = firstLSN;
        this.nextLSN = nextLSN;

        this.maxTransactionID = -1;

        incompleteTxns = new HashMap<Integer, LogSequenceNumber>();
    }


    /**
     * This helper method updates the recovery information with the
     * specified transaction ID and log sequence number.  The requirement is
     * that this method is only used during redo processing; we expect that
     * log sequence numbers are monotonically increasing.
     *
     * @param transactionID the ID of the transaction that appears in the
     *        current write-ahead log record
     *
     * @param lsn the log sequence number of the current write-ahead log
     *        record
     */
    public void updateInfo(int transactionID, LogSequenceNumber lsn) {
        incompleteTxns.put(transactionID, lsn);

        if (transactionID > maxTransactionID)
            maxTransactionID = transactionID;
    }


    /**
     * This helper method returns the last log sequence number seen for
     * the specified transaction.  It is used during undo processing to
     * allow subsequent WAL records to refer to the earlier WAL records
     * that appear for the transaction.
     *
     * @param transactionID the ID of the transaction to get the most
     *        recent LSN for
     *
     * @return the last log sequence number seen for the specified
     *         transaction
     */
    public LogSequenceNumber getLastLSN(int transactionID) {
        return incompleteTxns.get(transactionID);
    }


    /**
     * This helper function records that the specified transaction is
     * completed in the write-ahead log.  Specifically, the transaction is
     * removed from the set of incomplete transactions.
     *
     * @param transactionID the transaction to record as completed
     */
    public void recordTxnCompleted(int transactionID) {
        incompleteTxns.remove(transactionID);
    }


    /**
     * Returns true if there are any incomplete transactions, or false if
     * all transactions are completed.
     *
     * @return true if there are any incomplete transactions, or false if
     *         all transactions are completed.
     */
    public boolean hasIncompleteTxns() {
        return !incompleteTxns.isEmpty();
    }


    /**
     * Returns true if the specified transaction is complete, or false if
     * it appears in the set of incomplete transactions.
     *
     * @param transactionID the transaction to check for completion status
     *
     * @return true if the transaction is complete, or false otherwise
     */
    public boolean isTxnComplete(int transactionID) {
        return !incompleteTxns.containsKey(transactionID);
    }
}
