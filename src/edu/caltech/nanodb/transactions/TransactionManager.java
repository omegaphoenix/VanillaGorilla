package edu.caltech.nanodb.transactions;


import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;

import edu.caltech.nanodb.client.SessionState;

import edu.caltech.nanodb.expressions.TypeCastException;

import edu.caltech.nanodb.server.NanoDBServer;
import edu.caltech.nanodb.server.properties.PropertyHandler;
import edu.caltech.nanodb.server.properties.ReadOnlyPropertyException;
import edu.caltech.nanodb.server.properties.UnrecognizedPropertyException;

import edu.caltech.nanodb.storage.BufferManager;
import edu.caltech.nanodb.storage.BufferManagerObserver;
import edu.caltech.nanodb.storage.DBFile;
import edu.caltech.nanodb.storage.DBFileType;
import edu.caltech.nanodb.storage.DBPage;
import edu.caltech.nanodb.storage.StorageManager;

import edu.caltech.nanodb.storage.writeahead.LogSequenceNumber;
import edu.caltech.nanodb.storage.writeahead.RecoveryInfo;
import edu.caltech.nanodb.storage.writeahead.WALManager;
import edu.caltech.nanodb.storage.writeahead.WALRecordType;


/**
 */
public class TransactionManager implements BufferManagerObserver {
    /** A logging object for reporting anything interesting that happens. */
    private static Logger logger = Logger.getLogger(TransactionManager.class);


    /**
     * The system property that can be used to turn on or off transaction
     * processing.
     */
    public static final String PROP_TXNS = "nanodb.txns";


    /**
     * This is the name of the file that the Transaction Manager uses to keep
     * track of overall transaction state.
     */
    public static final String TXNSTATE_FILENAME = "txnstate.dat";


    /**
     * Returns true if the transaction processing system is enabled, or false
     * otherwise.
     *
     * @return true if the transaction processing system is enabled, or false
     *         otherwise.
     */
    public static boolean isEnabled() {
        // Transaction processing defaults to "off" until we actually
        // turn it on.
        return "on".equalsIgnoreCase(System.getProperty(PROP_TXNS, "off"));
    }


    private static class TransactionPropertyHandler implements PropertyHandler {

        @Override
        public Object getPropertyValue(String propertyName)
                throws UnrecognizedPropertyException {

            if (PROP_TXNS.equals(propertyName)) {
                return isEnabled();
            }
            else {
                throw new UnrecognizedPropertyException("No property named " +
                        propertyName);
            }
        }

        @Override
        public void setPropertyValue(String propertyName, Object value)
                throws UnrecognizedPropertyException, ReadOnlyPropertyException,
                TypeCastException {

            if (PROP_TXNS.equals(propertyName)) {
                throw new ReadOnlyPropertyException(propertyName +
                        " is read-only");
            }
            else {
                throw new UnrecognizedPropertyException("No property named " +
                        propertyName);
            }
        }
    }


    private NanoDBServer server;


    private StorageManager storageManager;


    private WALManager walManager;


    /**
     * This variable keeps track of the next transaction ID that should be used
     * for a transaction.  It is initialized when the transaction manager is
     * started.
     */
    private AtomicInteger nextTxnID;


    /**
     * This is the last value of nextLSN saved to the transaction-state file.
     */
    private LogSequenceNumber txnStateNextLSN;


    public TransactionManager(NanoDBServer server) {

        this.server = server;
        this.storageManager = server.getStorageManager();

        BufferManager bufferManager = storageManager.getBufferManager();

        // Register the transaction manager on the buffer manager so that we
        // can enforce the write-ahead logging rule for evicted pages.
        bufferManager.addObserver(this);

        this.nextTxnID = new AtomicInteger();

        walManager = new WALManager(storageManager, bufferManager);

        // Register properties that the Transaction Manager exposes.
        server.getPropertyRegistry().registerProperties(
            new TransactionPropertyHandler(), PROP_TXNS);

    }


    /**
     * This helper function initializes a brand new transaction-state file for
     * the transaction manager to use for providing transaction atomicity and
     * durability.
     *
     * @return a {@code DBFile} object for the newly created and initialized
     *         transaction-state file.
     *
     * @throws IOException if the transaction-state file can't be created for
     *         some reason.
     */
    private TransactionStatePage createTxnStateFile() throws IOException {
        // Create a brand new transaction-state file for the Transaction Manager
        // to use.

        DBFile dbfTxnState;
        dbfTxnState = storageManager.createDBFile(TXNSTATE_FILENAME,
            DBFileType.TXNSTATE_FILE);

        DBPage dbpTxnState = storageManager.loadDBPage(dbfTxnState, 0);
        TransactionStatePage txnState = new TransactionStatePage(dbpTxnState);

        // Set the "next transaction ID" value to an initial default.
        txnState.setNextTransactionID(1);
        nextTxnID.set(1);

        // Set the "first LSN" and "next LSN values to initial defaults.
        LogSequenceNumber lsn =
            new LogSequenceNumber(0, WALManager.OFFSET_FIRST_RECORD);

        txnState.setFirstLSN(lsn);
        txnState.setNextLSN(lsn);
        txnStateNextLSN = lsn;

        storageManager.getBufferManager().writeDBFile(dbfTxnState, /* sync */ true);

        return txnState;
    }


    private TransactionStatePage loadTxnStateFile() throws IOException {
        DBFile dbfTxnState = storageManager.openDBFile(TXNSTATE_FILENAME);
        DBPage dbpTxnState = storageManager.loadDBPage(dbfTxnState, 0);
        TransactionStatePage txnState = new TransactionStatePage(dbpTxnState);

        // Set the "next transaction ID" value properly.
        nextTxnID.set(txnState.getNextTransactionID());

        // Retrieve the "first LSN" and "next LSN values so we know the range of
        // the write-ahead log that we need to apply for recovery.
        txnStateNextLSN = txnState.getNextLSN();

        return txnState;
    }


    private void storeTxnStateToFile() throws IOException {
        DBFile dbfTxnState = storageManager.openDBFile(TXNSTATE_FILENAME);
        DBPage dbpTxnState = storageManager.loadDBPage(dbfTxnState, 0);
        TransactionStatePage txnState = new TransactionStatePage(dbpTxnState);

        txnState.setNextTransactionID(nextTxnID.get());
        txnState.setFirstLSN(walManager.getFirstLSN());
        txnState.setNextLSN(txnStateNextLSN);

        storageManager.getBufferManager().writeDBFile(dbfTxnState, /* sync */ true);
    }


    public void initialize() throws IOException {
        if (!isEnabled())
            throw new IllegalStateException("Transactions are disabled!");

        // Read the transaction-state file so we can initialize the
        // Transaction Manager.

        TransactionStatePage txnState;
        try {
            txnState = loadTxnStateFile();
        }
        catch (FileNotFoundException e) {
            // BUGBUG:  If we find any other files in the data directory, we
            //          really should fail initialization, because the old files
            //          may have been created without transaction processing...

            logger.info("Couldn't find transaction-state file " +
                TXNSTATE_FILENAME + ", creating.");

            txnState = createTxnStateFile();
        }

        // Perform recovery, and get the new "first LSN" value

        LogSequenceNumber firstLSN = txnState.getFirstLSN();
        LogSequenceNumber nextLSN = txnState.getNextLSN();
        logger.debug(String.format("Txn State has FirstLSN = %s, NextLSN = %s",
            firstLSN, nextLSN));

        RecoveryInfo recoveryInfo = walManager.doRecovery(firstLSN, nextLSN);

        // Set the "next transaction ID" value based on what recovery found
        int recNextTxnID = recoveryInfo.maxTransactionID + 1;
        if (recNextTxnID != -1 && recNextTxnID > nextTxnID.get()) {
            logger.info("Advancing NextTransactionID from " +
                nextTxnID.get() + " to " + recNextTxnID);
            nextTxnID.set(recNextTxnID);
        }

        // Update and sync the transaction state if any changes were made.
        storeTxnStateToFile();

        // Register the component that manages indexes when tables are modified.
        server.getEventDispatcher().addCommandEventListener(
            new TransactionStateUpdater(this));
    }


    /**
     * Returns the "next transaction ID" value without incrementing it.
     * This operation is thread-safe.
     *
     * @return the next transaction ID to use
     */
    public int getNextTxnID() {
        return nextTxnID.get();
    }


    /**
     * Returns the "next transaction ID" value, and also increments this
     * value.  This operation is thread-safe.
     *
     * @return the next transaction ID to use
     */
    public int getAndIncrementNextTxnID() {
        return nextTxnID.getAndIncrement();
    }


    public void startTransaction(boolean userStarted) throws TransactionException {
        SessionState state = SessionState.get();
        TransactionState txnState = state.getTxnState();

        if (txnState.isTxnInProgress())
            throw new IllegalStateException("A transaction is already in progress!");

        int txnID = getAndIncrementNextTxnID();
        txnState.setTransactionID(txnID);
        txnState.setUserStartedTxn(userStarted);

        logger.debug("Starting transaction with ID " + txnID +
            (userStarted ? " (user-started)" : ""));

        // Don't record a "start transaction" WAL record until the transaction
        // actually writes to something in the database.
    }


    public void recordPageUpdate(DBPage dbPage) throws IOException {
        if (!dbPage.isDirty()) {
            logger.debug("Page reports it is not dirty; not logging update.");
            return;
        }

        logger.debug("Recording page-update for page " + dbPage.getPageNo() +
            " of file " + dbPage.getDBFile());

        TransactionState txnState = SessionState.get().getTxnState();
        if (!txnState.hasLoggedTxnStart()) {
            walManager.writeTxnRecord(WALRecordType.START_TXN);
            txnState.setLoggedTxnStart(true);
        }

        walManager.writeUpdatePageRecord(dbPage);
        dbPage.syncOldPageData();
    }


    public void commitTransaction() throws TransactionException {
        SessionState state = SessionState.get();
        TransactionState txnState = state.getTxnState();

        if (!txnState.isTxnInProgress()) {
            // The user issued a COMMIT without starting a transaction!

            state.getOutputStream().println(
                "No transaction is currently in progress.");

            return;
        }

        int txnID = txnState.getTransactionID();

        if (txnState.hasLoggedTxnStart()) {
            // Must record the transaction as committed to the write-ahead log.
            // Then, we must force the WAL to include this commit record.
            try {
                walManager.writeTxnRecord(WALRecordType.COMMIT_TXN);
                forceWAL(walManager.getNextLSN());
            }
            catch (IOException e) {
                throw new TransactionException("Couldn't commit transaction " +
                    txnID + "!", e);
            }
        }
        else {
            logger.debug("Transaction " + txnID + " has made no changes; not " +
                "recording transaction-commit to WAL.");
        }

        // Now that the transaction is successfully committed, clear the current
        // transaction state.
        logger.debug("Transaction completed, resetting transaction state.");
        txnState.clear();
    }


    public void rollbackTransaction() throws TransactionException {
        SessionState state = SessionState.get();
        TransactionState txnState = state.getTxnState();

        if (!txnState.isTxnInProgress()) {
            // The user issued a ROLLBACK without starting a transaction!

            state.getOutputStream().println(
                "No transaction is currently in progress.");

            return;
        }

        int txnID = txnState.getTransactionID();

        if (txnState.hasLoggedTxnStart()) {
            // Must rollback the transaction using the write-ahead log.
            try {
                walManager.rollbackTransaction();
            }
            catch (IOException e) {
                throw new TransactionException(
                    "Couldn't rollback transaction " + txnID + "!", e);
            }
        }
        else {
            logger.debug("Transaction " + txnID + " has made no changes; not " +
                "recording transaction-rollback to WAL.");
        }

        // Now that the transaction is successfully rolled back, clear the
        // current transaction state.
        logger.debug("Transaction completed, resetting transaction state.");
        txnState.clear();
    }


    /**
     * This method is registered on the {@link BufferManager}, to ensure that
     * the write-ahead logging rule is enforced.  Specifically, all dirty
     * pages to be evicted from the buffer manager must be reflected in the
     * write-ahead log on disk, before they are evicted.
     *
     * @param pages the collection of pages that are about to be evicted.
     *
     * @throws IOException if an IO error occurs while trying to update the
     *         write-ahead log.
     */
    @Override
    public void beforeWriteDirtyPages(List<DBPage> pages) throws IOException {
        // TODO:  IMPLEMENT
        //
        // This implementation must enforce the write-ahead logging rule (aka
        // the WAL rule) by ensuring that the write-ahead log reflects all
        // changes to all of the specified pages, on disk, before any of these
        // pages may be written to disk.
        //
        // Recall that DBPages have a pageLSN field that is set to the LSN
        // of the last WAL record describing a change to the page.  This value
        // is not always set; it will be null if the page is part of a data
        // file whose type is not logged.  (It may also be null if there is a
        // bug in the write-ahead logging code.  It would be wise to report a
        // warning, or throw an exception, if a page doesn't have a LSN when
        // it ought to.)
        //
        // Some file types are not recorded to the write-ahead log; these
        // pages should be ignored when determining how to update the WAL.
        // You can find a page's file-type by doing something like this:
        // dbPage.getDBFile().getType().  If it is WRITE_AHEAD_LOG_FILE or
        // TXNSTATE_FILE then you should ignore the page.
        //
        // Finally, you can use the forceWAL(LogSequenceNumber) function to
        // force the WAL to be written out to the specified LSN.
    }


    /**
     * This method forces the write-ahead log out to at least the specified
     * log sequence number, syncing the log to ensure that all essential
     * records have reached the disk itself.
     *
     * @param lsn All WAL data up to this value must be forced to disk and
     *        sync'd.  This value may be one past the end of the current WAL
     *        file during normal operation.
     *
     * @throws IOException if an IO error occurs while attempting to force the
     *         WAL file to disk.  If a failure occurs, the database is probably
     *         going to be broken.
     */
    public void forceWAL(LogSequenceNumber lsn) throws IOException {
        // TODO:  IMPLEMENT
        //
        // Note that the "next LSN" value must be determined from both the
        // current LSN *and* its record size; otherwise we lose the last log
        // record in the WAL file.  You can use this static method:
        //
        // int lastPosition = lsn.getFileOffset() + lsn.getRecordSize();
        // WALManager.computeNextLSN(lsn.getLogFileNo(), lastPosition);
    }


    /**
     * This method forces the entire write-ahead log out to disk, syncing the
     * log as well.  This version is intended to be used during shutdown
     * processing in order to record all WAL changes to disk.
     *
     * @throws IOException if an IO error occurs while attempting to force the
     *         WAL file to disk.  If a failure occurs, the database is probably
     *         going to be broken.
     */
    public void forceWAL() throws IOException {
        forceWAL(walManager.getNextLSN());
    }
}
