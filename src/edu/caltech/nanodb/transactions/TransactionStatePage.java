package edu.caltech.nanodb.transactions;


import edu.caltech.nanodb.storage.DBPage;
import edu.caltech.nanodb.storage.writeahead.LogSequenceNumber;


/**
 * This class wraps the transaction-state page to provide basic operations
 * necessary for reading and storing essential values.  The values stored in
 * the transaction-state file are as follows:
 * <ul>
 * <li><b>Next Transaction ID.</b>  This is the next transaction ID, as
 *     recorded by the database at the last point this file was saved.</li>
 * <li><b>First Log Sequence Number (LSN).</b>  This is the LSN in the
 *     write-ahead log (WAL) where recovery should start from.  It denotes a
 *     point in time where all table files and the WAL are completely in sync
 *     with each other.  Since NanoDB doesn't support checkpointing (yet), this
 *     value is usually updated after recovery is completed, and also upon
 *     proper shutdown of the database.</li>
 * <li><b>Next LSN.</b>  This value is one byte past the last valid WAL record
 *     that has been successfully written <u>and sync'd</u> to the write-ahead
 *     log.  Note that this value may be behind the
 *     {@link edu.caltech.nanodb.storage.writeahead.WALManager#nextLSN} value
 *     stored in memory.</li>
 * </ul>
 */
public class TransactionStatePage {

    /**
     * The offset in the checkpoint page where the "Next Transaction ID" value
     * is stored.  This value is a signed int (4 bytes).
     */
    public static final int OFFSET_NEXT_TXN_ID = 2;


    /**
     * The offset in the checkpoint page where the "First Log Sequence Number"
     * file-number is stored.  This value is an unsigned short (2 bytes).
     */
    public static final int OFFSET_FIRST_LSN_FILENUM = 6;


    /**
     * The offset in the checkpoint page where the "First Log Sequence Number"
     * file-offset is stored.  This value is a signed int (4 bytes).
     */
    public static final int OFFSET_FIRST_LSN_OFFSET = 8;


    /**
     * The offset in the checkpoint page where the "Next Log Sequence Number"
     * file-number is stored.  This value is an unsigned short (2 bytes).
     */
    public static final int OFFSET_NEXT_LSN_FILENUM = 12;

    /**
     * The offset in the checkpoint page where the "Next Log Sequence Number"
     * file-offset is stored.  This value is a signed int (4 bytes).
     */
    public static final int OFFSET_NEXT_LSN_OFFSET = 14;


    private DBPage dbPage;


    public TransactionStatePage(DBPage dbPage) {
        this.dbPage = dbPage;
    }


    public int getNextTransactionID() {
        return dbPage.readInt(OFFSET_NEXT_TXN_ID);
    }


    public void setNextTransactionID(int nextTransactionID) {
        dbPage.writeInt(OFFSET_NEXT_TXN_ID, nextTransactionID);
    }


    public LogSequenceNumber getFirstLSN() {
        int fileNum = dbPage.readUnsignedShort(OFFSET_FIRST_LSN_FILENUM);
        int offset = dbPage.readInt(OFFSET_FIRST_LSN_OFFSET);

        return new LogSequenceNumber(fileNum, offset);
    }


    public void setFirstLSN(LogSequenceNumber firstLSN) {
        dbPage.writeShort(OFFSET_FIRST_LSN_FILENUM, firstLSN.getLogFileNo());
        dbPage.writeInt(OFFSET_FIRST_LSN_OFFSET, firstLSN.getFileOffset());
    }


    public LogSequenceNumber getNextLSN() {
        int fileNum = dbPage.readUnsignedShort(OFFSET_NEXT_LSN_FILENUM);
        int offset = dbPage.readInt(OFFSET_NEXT_LSN_OFFSET);

        return new LogSequenceNumber(fileNum, offset);
    }


    public void setNextLSN(LogSequenceNumber nextLSN) {
        dbPage.writeShort(OFFSET_NEXT_LSN_FILENUM, nextLSN.getLogFileNo());
        dbPage.writeInt(OFFSET_NEXT_LSN_OFFSET, nextLSN.getFileOffset());
    }
}
