package edu.caltech.nanodb.storage.writeahead;


/**
 * This class represents a Log Sequence Number (LSN) in the write-ahead log.
 * Every log record has a unique LSN value; furthermore, the LSN identifies the
 * exact location of the log record on disk as well.  Log Sequence Numbers are
 * comprised of the following parts:
 *
 * <ul>
 *   <li>The file number of the write-ahead log file for the record (range:
 *       00000..65535)</li>
 *   <li>The offset of the log record from the start of the file
 *       (range:  0..2<sup>31</sup>-1)</li>
 * </ul>
 */
public class LogSequenceNumber
    implements Comparable<LogSequenceNumber>, Cloneable {

    /** The number of the write-ahead log file that the record is stored in. */
    private int logFileNo;

    /** The offset of the log record from the start of the file. */
    private int fileOffset;


    /** The size of the record that this LSN points to, in bytes. */
    private int recordSize;


    public LogSequenceNumber(int logFileNo, int fileOffset) {
        if (logFileNo < 0 || logFileNo > WALManager.MAX_WAL_FILE_NUMBER) {
            throw new IllegalArgumentException(String.format(
                "WAL file numbers must be in the range [0, %d]; got %d instead.",
                WALManager.MAX_WAL_FILE_NUMBER, logFileNo));
        }

        if (fileOffset < 0)
            throw new IllegalArgumentException("File offset must be nonnegative");

        this.logFileNo = logFileNo;
        this.fileOffset = fileOffset;
        recordSize = 0;
    }


    @Override
    public boolean equals(Object obj) {
        if (obj instanceof LogSequenceNumber) {
            LogSequenceNumber lsn = (LogSequenceNumber) obj;
            return lsn.logFileNo == logFileNo && lsn.fileOffset == fileOffset;
        }
        return false;
    }


    /** Calculate a hash-code for this log-sequence number object. */
    @Override
    public int hashCode() {
        int hashCode;

        // Follows pattern in Effective Java, Item 8, with different constants.
        hashCode = 37;
        hashCode = 53 * hashCode + logFileNo;
        hashCode = 53 * hashCode + fileOffset;

        return hashCode;
    }


    public LogSequenceNumber clone() {
        try {
            return (LogSequenceNumber) super.clone();
        }
        catch (CloneNotSupportedException e) {
            // Should never happen.
            throw new RuntimeException(e);
        }
    }


    /**
     * Returns the file number of the WAL file that this record is in.
     *
     * @return the file number of the WAL file that this record is in.
     */
    public int getLogFileNo() {
        return logFileNo;
    }


    /**
     * Returns the offset from the start of the WAL file that this record
     * appears at.
     *
     * @return the offset from the start of the WAL file that this record
     *         appears at.
     */
    public int getFileOffset() {
        return fileOffset;
    }


    public void setRecordSize(int size) {
        recordSize = size;
    }


    public int getRecordSize() {
        return recordSize;
    }


    @Override
    public int compareTo(LogSequenceNumber lsn) {
        if (logFileNo != lsn.logFileNo)
            return logFileNo - lsn.logFileNo;

        return fileOffset - lsn.fileOffset;
    }


    @Override
    public String toString() {
        return String.format("LSN[%05d:%08d]", logFileNo, fileOffset);
    }
}
