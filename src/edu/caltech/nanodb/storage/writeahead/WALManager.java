package edu.caltech.nanodb.storage.writeahead;


import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.log4j.Logger;

import edu.caltech.nanodb.client.SessionState;
import edu.caltech.nanodb.storage.BufferManager;
import edu.caltech.nanodb.storage.DBFile;
import edu.caltech.nanodb.storage.DBFileReader;
import edu.caltech.nanodb.storage.DBFileType;
import edu.caltech.nanodb.storage.DBFileWriter;
import edu.caltech.nanodb.storage.DBPage;
import edu.caltech.nanodb.storage.StorageManager;
import edu.caltech.nanodb.transactions.TransactionManager;
import edu.caltech.nanodb.transactions.TransactionState;
import edu.caltech.nanodb.util.ArrayUtil;


/**
 * <p>
 * This class manages the write-ahead logs of the database.  There are methods
 * to write the different kinds of log records, and also to perform recovery
 * based on the contents of the write-ahead logs.  Note that many details of
 * transaction coordination are handled by the Transaction Manager, such as
 * syncing the write-ahead log, forcing the WAL, and so forth.
 * </p>
 * <p>
 * The actual details of the write-ahead log format are in the package Javadocs:
 * {@link edu.caltech.nanodb.storage.writeahead}.
 * </p>
 * <p>
 * Some of the {@code writeXXXX()} methods require explicit transaction details,
 * while others retrieve the transaction state from thread-local storage.  The
 * main difference is that methods that require explicit transaction details are
 * needed during recovery processing, when transaction state is dictated by the
 * log file, not what is in thread-local storage.
 * </p>
 */
public class WALManager {

    /** A logging object for reporting anything interesting that happens. */
    private static Logger logger = Logger.getLogger(WALManager.class);


    /** Write-ahead log files follow this pattern. */
    public static final String WAL_FILENAME_PATTERN = "wal-%05d.log";


    /**
     * Maximum file number for a write-ahead log file.
     */
    public static final int MAX_WAL_FILE_NUMBER = 65535;


    /**
     * Maximum size of a write-ahead log file is 10MB.  When the current WAL
     * file reaches this size, it is closed and a new WAL file is created with
     * the next increasing file number.
     */
    public static final int MAX_WAL_FILE_SIZE = 10 * 1024 * 1024;


    /**
     * This is the file-offset just past the last byte written in the previous
     * WAL file, or 0 for the first WAL file.  The value is an integer,
     * occupying 4 bytes.
     */
    public static final int OFFSET_PREV_FILE_END = 2;


    /**
     * This is the file-offset of the first log entry in a WAL file.
     */
    public static final int OFFSET_FIRST_RECORD = 6;


    /**
     * This static helper method simply takes a WAL file number and translates
     * it into a corresponding filename based on that number.
     *
     * @param fileNo the WAL file number to get the filename for
     *
     * @return the string file-name for the corresponding WAL file
     */
    public static String getWALFileName(int fileNo) {
        return String.format(WAL_FILENAME_PATTERN, fileNo);
    }


    private StorageManager storageManager;
    
    
    private BufferManager bufferManager;


    /**
     * This object holds the log sequence number of the first write-ahead log
     * record where recovery would need to start from.
     */
    private LogSequenceNumber firstLSN;


    /**
     * This object holds the log sequence number where the next write-ahead log
     * record will be written.
     */
    private LogSequenceNumber nextLSN;


    public WALManager(StorageManager storageManager,
                      BufferManager bufferManager) {
        this.storageManager = storageManager;
        this.bufferManager = bufferManager;
    }


    /**
     * This helper method creates a brand new write-ahead log file using the
     * Storage Manager, generating a suitable filename and passing the
     * appropriate arguments to the Storage Manager.
     * 
     * @param fileNo the number of the WAL file to create
     *
     * @return a {@link DBFile} for the newly created and opened WAL file
     *
     * @throws IOException if the file cannot be created for some reason
     */
    public DBFile createWALFile(int fileNo) throws IOException {
        String filename = getWALFileName(fileNo);
        logger.debug("Creating WAL file " + filename);
        return storageManager.createDBFile(filename, DBFileType.WRITE_AHEAD_LOG_FILE);
    }


    /**
     * This helper method opens an existing write-ahead log file using the
     * Storage Manager, generating a suitable filename and passing the
     * appropriate arguments to the Storage Manager.  Of course, if the file has
     * already been opened, the Storage Manager will return the {@code DBFile}
     * from its cache.
     *
     * @param fileNo the number of the WAL file to open
     *
     * @return a {@link DBFile} for the opened WAL file
     *
     * @throws IOException if the file cannot be opened for some reason, such
     *         as if the file doesn't exist, or if permissions are incorrect
     */
    public DBFile openWALFile(int fileNo) throws IOException {
        String filename = getWALFileName(fileNo);
        logger.debug("Opening WAL file " + filename);

        DBFile dbFile = storageManager.openDBFile(filename);
        DBFileType type = dbFile.getType();

        if (type != DBFileType.WRITE_AHEAD_LOG_FILE) {
            throw new IOException(String.format(
                "File %s is not of WAL-file type.", filename));
        }

        return dbFile;
    }


    public LogSequenceNumber getFirstLSN() {
        return firstLSN;
    }


    public LogSequenceNumber getNextLSN() {
        return nextLSN;
    }


    /**
     * Performs recovery processing starting at the specified log sequence
     * number, and returns the LSN where the next recovery process should start
     * from.
     *
     * @param storedFirstLSN the location of the write-ahead log record where
     *        recovery should start from
     *
     * @param storedNextLSN the location in the write-ahead log that is
     *        <em>just past</em> the last valid log record in the WAL
     *
     * @return the new location where recovery should start from the next time
     *         recovery processing is performed
     *         
     * @throws IOException if an IO error occurs during recovery processing
     */
    public RecoveryInfo doRecovery(LogSequenceNumber storedFirstLSN,
        LogSequenceNumber storedNextLSN) throws IOException {

        firstLSN = storedFirstLSN;
        nextLSN = storedNextLSN;
        RecoveryInfo recoveryInfo = new RecoveryInfo(firstLSN, nextLSN);

        if (firstLSN.equals(nextLSN)) {
            // No recovery necessary!  Just return the passed-in info.
            return recoveryInfo;
        }

        performRedo(recoveryInfo);
        performUndo(recoveryInfo);

        TransactionManager txnMgr = storageManager.getTransactionManager();

        // Force the WAL out, up to the nextLSN value.  Then, write all dirty
        // data pages, and sync all of the affected files.
        txnMgr.forceWAL(nextLSN);
        bufferManager.writeAll(true);

        // At this point, all files in the database should be in sync with
        // the entirety of the write-ahead log.  So, update the firstLSN value
        // and update the transaction state file again.  (This won't write out
        // any WAL records, but it will write and sync the txn-state file.)
        firstLSN = nextLSN;
        txnMgr.forceWAL(nextLSN);

        recoveryInfo.firstLSN = firstLSN;
        recoveryInfo.nextLSN = nextLSN;

        return recoveryInfo;
    }


    /**
     * This helper function performs redo processing using the write-ahead
     * log.  As the log is traversed, the <tt>RecoveryInfo</tt> object is also
     * updated with important redo/undo information.
     *
     * @param recoveryInfo the object used to track information about specific
     *        transactions during recovery processing.  This object will be
     *        passed to {@link #performUndo}.
     */
    private void performRedo(RecoveryInfo recoveryInfo) throws IOException {
        LogSequenceNumber currLSN = recoveryInfo.firstLSN;
        logger.debug("Starting redo processing at LSN " + currLSN);

        LogSequenceNumber oldLSN = null;
        DBFileReader walReader = null;
        while (currLSN.compareTo(recoveryInfo.nextLSN) < 0) {
            if (oldLSN == null || oldLSN.getLogFileNo() != currLSN.getLogFileNo())
                walReader = getWALFileReader(currLSN);

            // Read the parts of the log record that are always the same.
            byte typeID = walReader.readByte();
            WALRecordType type = WALRecordType.valueOf(typeID);

            int transactionID = walReader.readInt();

            logger.debug(String.format(
                "Redo:  examining WAL record at %s.  Type = %s, TxnID = %d",
                currLSN, type, transactionID));

            // TODO:  IMPLEMENT THE REST
            //
            //        Use logging statements liberally to help verify and
            //        debug your work.
            //
            //        If you encounter invalid WAL contents, throw a
            //        WALFileException to indicate the problem immediately.
            //
            //        You can use Java enums in a switch statement, like this:
            //
            //            switch (type) {
            //            case START_TXN:
            //                ...
            //
            //            case COMMIT_TXN:
            //                ...
            //
            //            default:
            //                throw new WALFileException(
            //                    "Encountered unrecognized WAL record type " +
            //                    type + " at LSN " + currLSN +
            //                    " during redo processing!");
            //            }

            oldLSN = currLSN;
            currLSN = computeNextLSN(currLSN.getLogFileNo(), walReader.getPosition());
        }

        if (currLSN.compareTo(recoveryInfo.nextLSN) != 0) {
            throw new WALFileException("Traversing WAL file didn't yield " +
                " the same ending LSN as in the transaction-state file.  WAL " +
                " result:  " + currLSN + "  TxnState:  " + recoveryInfo.nextLSN);
        }

        logger.debug("Redo processing is complete.  There are " +
            recoveryInfo.incompleteTxns.size() + " incomplete transactions.");
    }


    /**
     * This helper function performs undo processing using the write-ahead
     * log.  As the log is traversed backwards, the <tt>RecoveryInfo</tt>
     * object is also updated with important redo/undo information.
     *
     * @param recoveryInfo the object used to track information about specific
     *        transactions during recovery processing.  This object is
     *        populated by a previous call to {@link #performRedo}.
     */
    private void performUndo(RecoveryInfo recoveryInfo) throws IOException {
        LogSequenceNumber currLSN = recoveryInfo.nextLSN;
        logger.debug("Starting undo processing at " + currLSN);

        LogSequenceNumber oldLSN = null;
        DBFileReader walReader = null;
        while (recoveryInfo.hasIncompleteTxns()) {
            // Compute LSN of previous WAL record.  Start by getting the last
            // byte of the previous WAL record.
            int logFileNo = currLSN.getLogFileNo();
            int fileOffset = currLSN.getFileOffset();

            logger.debug("Finding record that comes before " + currLSN);

            // Wrap to the previous WAL file if necessary, and if there is one.
            if (fileOffset == OFFSET_FIRST_RECORD) {
                // Need to read the "previous WAL file's last offset" value
                // from the current WAL file.
                walReader = getWALFileReader(currLSN);
                walReader.setPosition(OFFSET_PREV_FILE_END);
                int prevFileEndOffset = walReader.readInt();
                if (prevFileEndOffset == 0) {
                    logger.debug("Reached the very start of the write-ahead log!");
                    break;
                }

                // Need to go back to the previous WAL file.
                logFileNo--;
                if (logFileNo < 0)  // Did we wrap around?
                    logFileNo = MAX_WAL_FILE_NUMBER;

                currLSN = new LogSequenceNumber(logFileNo, prevFileEndOffset);
                fileOffset = currLSN.getFileOffset();
            }
            else if (fileOffset < OFFSET_FIRST_RECORD) {
                // This would be highly unusual, but would indicate either a
                // bug in the undo record-traversal, or a corrupt WAL file.
                throw new WALFileException(String.format("Overshot the start " +
                    "of WAL file %d's records; ended up at file-position %d",
                    logFileNo, fileOffset));
            }

            if (currLSN.compareTo(recoveryInfo.firstLSN) <= 0)
                break;

            if (oldLSN == null || oldLSN.getLogFileNo() != logFileNo)
                walReader = getWALFileReader(currLSN);
            else
                walReader.setPosition(currLSN.getFileOffset());

            // Move backward one byte in the WAL file to read the previous
            // record's type ID.
            walReader.movePosition(-1);
            byte typeID = walReader.readByte();
            WALRecordType type = WALRecordType.valueOf(typeID);

            // Compute the start of the previous record based on its type and
            // other details.
            int startOffset;
            switch (type) {
            case START_TXN:
                // Type (1B) + TransactionID (4B) + Type (1B) = 6 bytes
                startOffset = fileOffset - 6;
                break;

            case COMMIT_TXN:
            case ABORT_TXN:
                // Type (1B) + TransactionID (4B) + PrevLSN (2B+4B) + Type (1B)
                // = 12 bytes
                startOffset = fileOffset - 12;
                break;

            case UPDATE_PAGE:
            case UPDATE_PAGE_REDO_ONLY:
                // For these records, the WAL record's start offset is stored
                // immediately before the last type-byte.  We go back 5 bytes
                // because reading the type ID moves the position forward by
                // 1 byte, and then we also have to get to the start of the
                // 4-byte starting offset.
                walReader.movePosition(-5);
                startOffset = walReader.readInt();
                break;

            default:
                throw new WALFileException(
                    "Encountered unrecognized WAL record type " + type +
                        " at LSN " + currLSN + " during redo processing!");
            }

            // Construct a new LSN pointing to the previous record.  If this
            // happens to be before the range that we are using for recovery,
            // we're done with undo-processing.
            currLSN = new LogSequenceNumber(logFileNo, startOffset);
            if (currLSN.compareTo(recoveryInfo.firstLSN) < 0)
                break;

            // Skip over the "record type" byte, which is at startOffset.
            // This sets up to read the transaction ID, next.
            walReader.setPosition(startOffset + 1);

            // Read the transaction ID.
            int transactionID = walReader.readInt();
            if (recoveryInfo.isTxnComplete(transactionID)) {
                // The current transaction is already completed, so skip the
                // record.
                oldLSN = currLSN;
                continue;
            }

            // Undo specific operations.  Note that we don't have to set the
            // reader's position to anything special at the end of each record,
            // since the above code will always properly move to the appropriate
            // position for the previous record, based on the value of currLSN.

            logger.debug(String.format(
                "Undo:  examining WAL record at %s.  Type = %s, TxnID = %d",
                currLSN, type, transactionID));

            // TODO:  IMPLEMENT THE REST
            //
            //        Use logging statements liberally to help verify and
            //        debug your work.
            //
            //        If you encounter invalid WAL contents, throw a
            //        WALFileException to indicate the problem immediately.
            //
            //        You can use Java enums in a switch statement, like this:
            //
            //            switch (type) {
            //            case START_TXN:
            //                ...
            //
            //            case COMMIT_TXN:
            //                ...
            //
            //            default:
            //                throw new WALFileException(
            //                    "Encountered unrecognized WAL record type " +
            //                    type + " at LSN " + currLSN +
            //                    " during undo processing!");
            //            }

            oldLSN = currLSN;
        }

        logger.debug("Undo processing is complete.");
    }


    /**
     * This static helper function takes the file number of a WAL file, and
     * the offset in the WAL file where the next write-ahead log record would
     * go if the WAL file can hold more data, and then creates a new
     * {@code LogSequenceNumber} object, wrapping to the next file if
     * necessary.
     *
     * @param fileNo the number of the current WAL file
     * @param fileOffset the offset of where the next write-ahead log record
     *        would go, in the absence of wrapping to the next file
     * @return a {@code LogSequenceNumber} object that takes wrapping into
     *         account
     */
    public static LogSequenceNumber computeNextLSN(int fileNo, int fileOffset) {
        if (fileOffset >= MAX_WAL_FILE_SIZE) {
            // This WAL file has reached the size limit.  Increment the file
            // number, wrapping around if necessary, and reset the offset to 0.
            fileNo += 1;
            if (fileNo > MAX_WAL_FILE_NUMBER)
                fileNo = 0;

            // Need to make sure we skip past the file type and size at the
            // start of the data file.
            fileOffset = OFFSET_FIRST_RECORD;
        }
        return new LogSequenceNumber(fileNo, fileOffset);
    }


    /**
     * This method opens the WAL file specified in the passed-in Log Sequence
     * Number, wraps it with a {@link DBFileWriter} so that it can be read and
     * written, and then seeks to the specified file offset.
     *
     * Because we are writing, the file may not yet exist.  In that case, this
     * method will also create a new WAL file for the specified file number,
     * initializing it with the proper values, and then seeking to the location
     * where the first WAL record may be written.
     *
     * @param lsn The log sequence number specifying the WAL file and the offset
     *            in the WAL file to go to.
     *
     * @return the WAL file, with the file position moved to the specified
     *         offset.
     *
     * @throws IOException if the corresponding WAL file cannot be opened, or
     *         if some other IO error occurs.
     */
    private DBFileWriter getWALFileWriter(LogSequenceNumber lsn)
        throws IOException {

        int fileNo = lsn.getLogFileNo();
        int offset = lsn.getFileOffset();

        DBFile walFile;
        try {
            walFile = openWALFile(fileNo);
        }
        catch (FileNotFoundException e) {
            logger.debug("WAL file doesn't exist!  WAL is expanding into a new file.");
            walFile = createWALFile(fileNo);
            // TODO:  Write the previous WAL file's last file-offset into the new WAL file's start.
        }

        DBFileWriter writer = new DBFileWriter(walFile, storageManager);
        writer.setPosition(offset);

        return writer;
    }


    /**
     * This method opens the WAL file specified in the passed-in Log Sequence
     * Number, wraps it with a {@link DBFileReader} so that it can be read from,
     * and then seeks to the specified file offset.
     *
     * Since we are reading, the expectation is that the file already
     * exists, so a {@link java.io.FileNotFoundException} will be thrown if it
     * does not exist.
     *
     * @param lsn The log sequence number specifying the WAL file and the offset
     *            in the WAL file to go to.
     *
     * @return the WAL file, with the file position moved to the specified
     *         offset.
     *
     * @throws IOException if an IO error occurs while opening the WAL file,
     *         such as the required file not actually existing.
     */
    private DBFileReader getWALFileReader(LogSequenceNumber lsn)
        throws IOException {

        int fileNo = lsn.getLogFileNo();
        int offset = lsn.getFileOffset();

        DBFile walFile = openWALFile(fileNo);
        DBFileReader reader = new DBFileReader(walFile, storageManager);
        reader.setPosition(offset);

        return reader;
    }


    /**
     * This function writes a transaction demarcation record
     * ({@link WALRecordType#START_TXN}, {@link WALRecordType#COMMIT_TXN}, or
     * {@link WALRecordType#ABORT_TXN}) to the write-ahead log.  The transaction
     * state is passed explicitly so that this method can be used during
     * recovery processing.  The alternate method
     * {@link #writeTxnRecord(WALRecordType)} retrieves the transaction state
     * from thread-local storage, and should be used during normal operation.
     *
     * @param type The type of the transaction demarcation to write, one of the
     *        values {@link WALRecordType#START_TXN}, {@link WALRecordType#COMMIT_TXN},
     *        or {@link WALRecordType#ABORT_TXN}.
     *
     * @param transactionID the transaction ID that the WAL record is for
     *
     * @param prevLSN the log sequence number of the transaction's immediately
     *        previous WAL record, if the record type is either a commit or
     *        abort record.
     *
     * @return the Log Sequence Number of the WAL record that was written
     *
     * @throws IOException if the write-ahead log can't be updated for some
     *         reason.
     *
     * @throws IllegalArgumentException if <tt>type</tt> is <tt>null</tt>, or if
     *         it isn't one of the values {@link WALRecordType#START_TXN},
     *         {@link WALRecordType#COMMIT_TXN}, or {@link WALRecordType#ABORT_TXN}.
     */
    public LogSequenceNumber writeTxnRecord(WALRecordType type,
        int transactionID, LogSequenceNumber prevLSN) throws IOException {

        if (type != WALRecordType.START_TXN &&
            type != WALRecordType.COMMIT_TXN &&
            type != WALRecordType.ABORT_TXN) {
            throw new IllegalArgumentException("Invalid record type " + type +
                " passed to writeTxnRecord().");
        }

        if ((type == WALRecordType.COMMIT_TXN ||
             type == WALRecordType.ABORT_TXN) && prevLSN == null) {
            throw new IllegalArgumentException(
                "prevLSN must be specified for records of type " + type);
        }

        LogSequenceNumber lsn = nextLSN;

        logger.debug("Writing a " + type + " record for transaction " +
            transactionID + " at LSN " + lsn);

        // Record the WAL record.  First thing to do:  figure out where it goes.

        DBFileWriter walWriter = getWALFileWriter(lsn);

        walWriter.writeByte(type.getID());
        walWriter.writeInt(transactionID);

        if (type == WALRecordType.START_TXN) {
            walWriter.writeByte(type.getID());

            // TypeID (1B) + TransactionID (4B) + TypeID (1B)
            lsn.setRecordSize(6);
        }
        else {
            walWriter.writeShort(prevLSN.getLogFileNo());
            walWriter.writeInt(prevLSN.getFileOffset());
            walWriter.writeByte(type.getID());

            // TypeID (1B) + TransactionID (4B) + PrevLSN (6B) + TypeID (1B)
            lsn.setRecordSize(12);
        }

        nextLSN = computeNextLSN(nextLSN.getLogFileNo(), walWriter.getPosition());
        logger.debug("Next-LSN value is now " + nextLSN);

        return lsn;
    }


    /**
     * This function writes a transaction demarcation record
     * ({@link WALRecordType#START_TXN}, {@link WALRecordType#COMMIT_TXN}, or
     * {@link WALRecordType#ABORT_TXN}) to the write-ahead log.  The transaction
     * state is retrieved from thread-local storage so that it doesn't need to
     * be passed.
     *
     * @param type The type of the transaction demarcation to write, one of the
     *        values {@link WALRecordType#START_TXN}, {@link WALRecordType#COMMIT_TXN},
     *        or {@link WALRecordType#ABORT_TXN}.
     *
     * @return the Log Sequence Number of the WAL record that was written
     *
     * @throws IOException if the write-ahead log can't be updated for some
     *         reason.
     *
     * @throws IllegalArgumentException if <tt>type</tt> is <tt>null</tt>, or if
     *         it isn't one of the values {@link WALRecordType#START_TXN},
     *         {@link WALRecordType#COMMIT_TXN}, or {@link WALRecordType#ABORT_TXN}.
     */
    public LogSequenceNumber writeTxnRecord(WALRecordType type)
        throws IOException {

        // Retrieve and verify the transaction state.
        TransactionState txnState = SessionState.get().getTxnState();
        if (!txnState.isTxnInProgress()) {
            throw new IllegalStateException(
                "No transaction is currently in progress!");
        }

        LogSequenceNumber lsn = writeTxnRecord(type, txnState.getTransactionID(),
            txnState.getLastLSN());

        txnState.setLastLSN(lsn);

        return lsn;
    }


    /**
     * This method writes an update-page record to the write-ahead log,
     * including both undo and redo details.
     *
     * @param dbPage The data page whose changes are to be recorded in the log.
     *
     * @return the Log Sequence Number of the WAL record that was written
     *
     * @throws IOException if the write-ahead log cannot be updated for some
     *         reason.
     * 
     * @throws IllegalArgumentException if <tt>dbPage</tt> is <tt>null</tt>, or
     *         if it shows no updates.
     */
    public LogSequenceNumber writeUpdatePageRecord(DBPage dbPage)
        throws IOException {

        if (dbPage == null)
            throw new IllegalArgumentException("dbPage must be specified");

        if (!dbPage.isDirty())
            throw new IllegalArgumentException("dbPage has no updates to store");

        // Retrieve and verify the transaction state.
        TransactionState txnState = SessionState.get().getTxnState();
        if (!txnState.isTxnInProgress()) {
            throw new IllegalStateException(
                "No transaction is currently in progress!");
        }

        LogSequenceNumber lsn = nextLSN;

        logger.debug(String.format("Writing an %s record for transaction %d at LSN %s",
            WALRecordType.UPDATE_PAGE, txnState.getTransactionID(), lsn));

        // Record the WAL record.  First thing to do:  figure out where it goes.

        DBFileWriter walWriter = getWALFileWriter(lsn);

        walWriter.writeByte(WALRecordType.UPDATE_PAGE.getID());
        walWriter.writeInt(txnState.getTransactionID());

        // We need to store the previous log sequence number for this record.
        LogSequenceNumber prevLSN = txnState.getLastLSN();
        walWriter.writeShort(prevLSN.getLogFileNo());
        walWriter.writeInt(prevLSN.getFileOffset());

        // Store the filename and page number that is being updated.
        walWriter.writeVarString255(dbPage.getDBFile().getDataFile().getName());
        walWriter.writeShort(dbPage.getPageNo());

        // This offset is where we will store the number of data segments we
        // need to record.  We don't know the value until later, so remember
        // the position and fill it in later.
        int segCountOffset = walWriter.getPosition();
        walWriter.writeShort(-1);
        
        byte[] oldData = dbPage.getOldPageData();
        byte[] newData = dbPage.getPageData();
        int pageSize = dbPage.getPageSize();

        // DEBUG:  Show changes from old version of page to new version of page.
        // logger.debug("DBPage changes:\n" + dbPage.getChangesAsString());

        int numSegments = 0;
        int index = 0;
        while (index < pageSize) {
            logger.debug("Skipping identical bytes starting at index " + index);
            
            // Skip data until we find stuff that's different.
            index += ArrayUtil.sizeOfIdenticalRange(oldData, newData, index);
            assert index <= pageSize;
            if (index == pageSize)
                break;

            logger.debug("Recording changed bytes starting at index " + index);

            // Find out how much data is actually changed.  We lump in small
            // runs of unchanged data just to make things more efficient.
            int size = 0;
            while (index + size < pageSize) {
                size += ArrayUtil.sizeOfDifferentRange(oldData, newData,
                    index + size);
                assert index + size <= pageSize;
                if (index + size == pageSize)
                    break;

                // If there are 4 or less identical bytes after the different
                // bytes, include them in this segment.
                int sameSize = ArrayUtil.sizeOfIdenticalRange(oldData, newData,
                    index + size);

                if (sameSize > 4 || index + size + sameSize == pageSize)
                    break;

                size += sameSize;
            }

            logger.debug("Found " + size + " changed bytes starting at index " +
                index);

            // Write the starting index within the page, and the amount of
            // data that will be recorded at that index.
            walWriter.writeShort(index);
            walWriter.writeShort(size);

            // Write the old data (undo), and then the new data (redo).
            walWriter.write(oldData, index, size);
            walWriter.write(newData, index, size);

            numSegments++;

            index += size;
        }
        assert index == pageSize;

        // Now that we know how many segments were recorded, store that value
        // at the appropriate location.
        int currOffset = walWriter.getPosition();
        walWriter.setPosition(segCountOffset);
        walWriter.writeShort(numSegments);
        walWriter.setPosition(currOffset);

        // Write the start of the update record at the end so that we can get
        // back to the record's start when scanning the log backwards.

        walWriter.writeInt(lsn.getFileOffset());
        walWriter.writeByte(WALRecordType.UPDATE_PAGE.getID());

        // Store the LSN of the change on the page.
        lsn.setRecordSize(walWriter.getPosition() - lsn.getFileOffset());
        dbPage.setPageLSN(lsn);
        dbPage.syncOldPageData();

        // Since we issued a new write-ahead log record for the current
        // transaction, update the "last LSN" value for the transaction.
        txnState.setLastLSN(lsn);

        nextLSN = computeNextLSN(nextLSN.getLogFileNo(), walWriter.getPosition());

        return lsn;
    }


    /**
     * This helper function writes a sequence of redo-segments from an
     * {@link WALRecordType#UPDATE_PAGE} or
     * {@link WALRecordType#UPDATE_PAGE_REDO_ONLY} record.  Note that the
     * {@code walReader} argument is expected to be positioned at the start of
     * the segments containing the old and new versions of the page data (or
     * just the new versions, for redo-only records).  Additionally, the
     * reader position will be advanced by this method.
     *
     * @param type The record type, either {@link WALRecordType#UPDATE_PAGE}
     *        or {@link WALRecordType#UPDATE_PAGE_REDO_ONLY}.
     *
     * @param walReader A reader positioned at the start of the redo/undo data
     *        to apply to the data page.  This method will advance the reader's
     *        position past this redo/undo data.
     *
     * @param dbPage the page that the redo should be applied to
     * @param numSegments the number of segments containing redo[/undo] data;
     *        this value is expected to already be unpacked from the log record
     * @throws IOException
     */
    private void applyRedo(WALRecordType type, DBFileReader walReader,
                           DBPage dbPage, int numSegments) throws IOException {

        if (type != WALRecordType.UPDATE_PAGE &&
            type != WALRecordType.UPDATE_PAGE_REDO_ONLY) {
            throw new IllegalArgumentException("This method can only be " +
                "used with UPDATE_PAGE and UPDATE_PAGE_REDO_ONLY records.");
        }

        for (int iSeg = 0; iSeg < numSegments; iSeg++) {
            // Write the starting index within the page, and the amount of
            // data that will be recorded at that index.
            int index = walReader.readUnsignedShort();
            int size = walReader.readUnsignedShort();

            // If it's an UPDATE_PAGE record, skip over the undo data.
            if (type == WALRecordType.UPDATE_PAGE)
                walReader.movePosition(size);

            // Write the redo data into the page.
            byte[] redoData = new byte[size];
            walReader.read(redoData);
            dbPage.write(index, redoData);
        }
    }


    /**
     * This helper method uses a {@link WALRecordType#UPDATE_PAGE} record to
     * undo changes to a data page, and at the same time the method generates
     * the data that must go into a corresponding redo-only WAL record.
     *
     * @param walReader A reader positioned at the start of the redo/undo data
     *        to apply to the data page.  This method will advance the reader's
     *        position past this redo/undo data.
     *
     * @param dbPage the data page that undo operations should be applied to
     *
     * @param numSegments the number of segments in the redo/undo data
     *
     * @return a byte-array containing the same number of segments as the
     *         original update record, with only the data necessary for the
     *         redo-only record.
     *
     * @throws IOException if an IO error occurs while applying the undo
     *         operation
     */
    private byte[] applyUndoAndGenRedoOnlyData(DBFileReader walReader,
        DBPage dbPage, int numSegments) throws IOException {

        ByteArrayOutputStream redoOnlyBAOS = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(redoOnlyBAOS);

        for (int i = 0; i < numSegments; i++) {
            // Read the starting index and length of this segment.
            int index = walReader.readUnsignedShort();
            int size = walReader.readUnsignedShort();

            // Apply the undo data to the data page.
            byte[] undoData = new byte[size];
            walReader.read(undoData);
            dbPage.write(index, undoData);

            // Skip past the redo data, because don't care about it.
            walReader.movePosition(size);

            // Record what we wrote into the redo-only record data.
            dos.writeShort(index);
            dos.writeShort(size);
            dos.write(undoData);
        }

        // Return the data that will appear in the redo-only record body.
        dos.flush();
        return redoOnlyBAOS.toByteArray();
    }


    /**
     * This method writes a redo-only update-page record to the write-ahead log,
     * including only redo details.  The transaction state is passed explicitly
     * so that this method can be used during recovery processing.  The
     * alternate method
     * {@link #writeRedoOnlyUpdatePageRecord(DBPage, int, byte[])} retrieves
     * the transaction state from thread-local storage, and should be used
     * during normal operation.
     *
     * @param transactionID the transaction ID that the WAL record is for.
     *
     * @param prevLSN the log sequence number of the transaction's immediately
     *        previous WAL record.
     *
     * @param dbPage The data page whose changes are to be recorded in the log.
     *
     * @param numSegments The number of segments in the change-data to record.
     *
     * @param changes The actual changes themselves, serialized to a byte array.

     * @return the Log Sequence Number of the WAL record that was written
     *
     * @throws IOException if the write-ahead log cannot be updated for some
     *         reason.
     *
     * @throws IllegalArgumentException if <tt>dbPage</tt> is <tt>null</tt>, or
     *         if <tt>changes</tt> is <tt>null</tt>.
     */
    public LogSequenceNumber writeRedoOnlyUpdatePageRecord(int transactionID,
        LogSequenceNumber prevLSN, DBPage dbPage, int numSegments,
        byte[] changes) throws IOException {

        if (dbPage == null)
            throw new IllegalArgumentException("dbPage must be specified");

        if (changes == null)
            throw new IllegalArgumentException("changes must be specified");

        // Record the WAL record.  First thing to do:  figure out where it goes.

        LogSequenceNumber lsn = nextLSN;
        
        logger.debug(String.format("Writing redo-only update record for " +
            "transaction %d at LSN %s.  PrevLSN = %s", transactionID, lsn, prevLSN));

        DBFileWriter walWriter = getWALFileWriter(lsn);

        walWriter.writeByte(WALRecordType.UPDATE_PAGE_REDO_ONLY.getID());
        walWriter.writeInt(transactionID);

        // We need to store the previous log sequence number for this record.
        walWriter.writeShort(prevLSN.getLogFileNo());
        walWriter.writeInt(prevLSN.getFileOffset());

        walWriter.writeVarString255(dbPage.getDBFile().getDataFile().getName());
        walWriter.writeShort(dbPage.getPageNo());

        // Write the redo-only data.
        walWriter.writeShort(numSegments);
        walWriter.write(changes);

        // Write the start of the update record at the end so that we can get
        // back to the record's start when scanning the log backwards.

        walWriter.writeInt(lsn.getFileOffset());
        walWriter.writeByte(WALRecordType.UPDATE_PAGE_REDO_ONLY.getID());

        // Store the LSN of the change on the page.
        lsn.setRecordSize(walWriter.getPosition() - lsn.getFileOffset());
        dbPage.setPageLSN(lsn);
        dbPage.syncOldPageData();

        nextLSN = computeNextLSN(nextLSN.getLogFileNo(), walWriter.getPosition());

        return lsn;
    }


    /**
     * This method writes a redo-only update-page record to the write-ahead log,
     * including only redo details.  The transaction state is taken from
     * thread-local storage; this method should be used during normal operation.
     *
     * The alternate method {@link #writeRedoOnlyUpdatePageRecord(int,
     * LogSequenceNumber, DBPage, int, byte[])} should be used during recovery
     * processing when transaction state is specifically known.
     *
     * @param dbPage The data page whose changes are to be recorded in the log.
     *
     * @param numSegments The number of segments in the change-data to record.
     *
     * @param changes The actual changes themselves, serialized to a byte array.

     * @return the Log Sequence Number of the WAL record that was written
     *
     * @throws IOException if the write-ahead log cannot be updated for some
     *         reason.
     *
     * @throws IllegalArgumentException if <tt>dbPage</tt> is <tt>null</tt>, or
     *         if <tt>changes</tt> is <tt>null</tt>.
     */
    public LogSequenceNumber writeRedoOnlyUpdatePageRecord(DBPage dbPage,
        int numSegments, byte[] changes) throws IOException {

        // Retrieve and verify the transaction state.
        TransactionState txnState = SessionState.get().getTxnState();
        if (!txnState.isTxnInProgress()) {
            throw new IllegalStateException(
                "No transaction is currently in progress!");
        }

        LogSequenceNumber lsn =
            writeRedoOnlyUpdatePageRecord(txnState.getTransactionID(),
            txnState.getLastLSN(), dbPage, numSegments, changes);

        txnState.setLastLSN(lsn);

        return lsn;
    }


    /**
     * This method performs the operations necessary to rollback the current
     * transaction from the database.  The transaction details are taken from
     * the transaction state stored in thread-local storage.  This method is
     * not used during recovery processing; the {@link #performUndo} method is
     * used to rollback all incomplete transactions in the logs.
     *
     * @throws IOException if an IO error occurs during rollback.
     */
    public void rollbackTransaction() throws IOException {
        // Get the details for the transaction to rollback.
        TransactionState txnState = SessionState.get().getTxnState();

        int transactionID = txnState.getTransactionID();
        if (transactionID == TransactionState.NO_TRANSACTION) {
            logger.info("No transaction in progress - rollback is a no-op.");
            return;
        }

        LogSequenceNumber lsn = txnState.getLastLSN();

        logger.info("Rolling back transaction " + transactionID +
            ".  Last LSN = " + lsn);

        // Scan backward through the log records for this transaction to roll
        // it back.
        
        while (true) {
            DBFileReader walReader = getWALFileReader(lsn);

            WALRecordType type = WALRecordType.valueOf(walReader.readByte());
            int recordTxnID = walReader.readInt();
            if (recordTxnID != transactionID) {
                throw new WALFileException(String.format("Sent to WAL record " +
                    "for transaction %d at LSN %s, during rollback of " +
                    "transaction %d.", recordTxnID, lsn, transactionID));
            }

            logger.debug(String.format(
                "Undoing WAL record at %s.  Type = %s, TxnID = %d",
                lsn, type, transactionID));

            // TODO:  IMPLEMENT THE REST
            //
            //        Use logging statements liberally to help verify and
            //        debug your work.
            //
            //        If you encounter invalid WAL contents, throw a
            //        WALFileException to indicate the problem immediately.
            //
            // TODO:  SET lsn TO PREVIOUS LSN TO WALK BACKWARD THROUGH WAL.

            // TODO:  This break is just here so the code will compile; when
            //        you provide your own implementation, get rid of it!
            break;
        }

        // All done rolling back the transaction!  Record that it was aborted
        // in the WAL.
        writeTxnRecord(WALRecordType.ABORT_TXN);
        logger.info(String.format("Transaction %d:  Rollback complete.",
            transactionID));
    }
}
