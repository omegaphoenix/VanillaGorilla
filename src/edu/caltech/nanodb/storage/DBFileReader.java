package edu.caltech.nanodb.storage;


import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.UnsupportedEncodingException;


/**
 * <p>
 * This class provides the basic abilty to read a {@link DBFile} as a single
 * sequential file, ignoring the fact that it is actually broken into pages.
 * As the file-pointer moves through the file, the Storage Manager is used to
 * load individual pages into the buffer manager.
 * </p>
 * <p>
 * It is certainly possible that a value being read might span two adjacent
 * data pages.  In these cases, the read will be a little slower, as the reader
 * must read partial data from the first page, and then load and read the
 * remainder of the data from the next page.
 * </p>
 * <p>
 * There is a subclass of this reader, the {@link DBFileWriter}, which allows
 * data to be read and written sequentially to a {@link DBFile}.  This is mainly
 * provided to support write-ahead logging, which is generally a very sequential
 * operation.
 * </p>
 */
public class DBFileReader {
    /** A logging object for reporting anything interesting that happens. */
    private static Logger logger = Logger.getLogger(DBFileReader.class);


    /**
     * A reference to the storage manager, which we will use to read pages
     * needed for the various operations.
     */
    protected StorageManager storageManager;


    /** The database file being read by this reader. */
    protected DBFile dbFile;


    /**
     * A flag controlling whether the file being read should be extended as
     * it is read.  The reader is expected to not extend the file, but the
     * {@link DBFileWriter}, a subclass of this class, sets this flag to true.
     */
    protected boolean extendFile = false;


    /** The page-size of the database file being read from. */
    protected int pageSize;


    /** The last page used for reading the database file. */
    protected DBPage dbPage;
    
    
    /** The current position in the file where reads will occur from. */
    protected int position;


    /**
     * This temporary buffer is used to read primitive values that overlap the
     * boundaries between two pages.
     */
    protected byte[] tmpBuf = new byte[8];


    public DBFileReader(DBFile dbFile, StorageManager storageManager) {
        if (dbFile == null)
            throw new IllegalArgumentException("dbFile cannot be null");

        this.storageManager = storageManager;

        this.dbFile = dbFile;
        position = 0;

        // The presumption is that page sizes are a power of two.
        pageSize = dbFile.getPageSize();
    }


    public DBFile getDBFile() {
        return dbFile;
    }


    /**
     * Returns the current location in the page where the next operation will
     * start from.
     *
     * @return the current location in the page
     */
    public int getPosition() {
        return position;
    }


    /**
     * Sets the location in the page where the next operation will start from.
     *
     * @param position the new location in the page
     */
    public void setPosition(int position) {
        if (position < 0) {
            throw new IllegalArgumentException("position must be >= 0, got " +
                position);
        }

        this.position = position;
    }


    /**
     * Move the current position by <tt>n</tt> bytes.  A negative value of
     * <tt>n</tt> will move the position backward.
     *
     * @param n the delta to apply to the current position
     */
    public void movePosition(int n) {
        if (position + n < 0)
            throw new IllegalArgumentException("can't move position before file start");

        position += n;
    }


    /**
     * Computes and returns the page-number of the page that the
     * {@code position} value currently falls within.
     *
     * @return the page-number of the page that the {@code position} value
     *         currently falls within
     */
    protected int getPositionPageNo() {
        return position / pageSize;
    }


    /**
     * Computes and returns the offset within the current page that the
     * {@code position} value currently falls at.
     *
     * @return the offset within the current page that the {@code position}
     *         value currently falls at
     */
    protected int getPositionPageOffset() {
        return position % pageSize;
    }


    /**
     * This helper function checks to see whether the currently cached
     * {@link DBPage} object matches the page-number of the current
     * {@link #position} value.  If they don't match, or if there currently
     * isn't any {@code DBPage} cached, then this method will load the required
     * {@code DBPage} from the Storage Manager.
     *
     * @throws IOException if an IO error occurs while reading the required
     *         database page
     */
    protected void checkDBPage() throws IOException {
        int pageNo = getPositionPageNo();

        // If the current DBPage is the one we need, use it!
        if (dbPage != null && dbPage.getPageNo() == pageNo)
            return;

        // Need to load the required DBPage.
        dbPage = storageManager.loadDBPage(dbFile, pageNo, extendFile);
    }


    /**
     * This method causes the writer to set the current {@link DBPage} to
     * {@code null}, so that the reader doesn't hold on to a page that might
     * need to be flushed out of the buffer manager.
     *
     * @todo Once we have page-pinning, this will also need to unpin the page.
     */
    public void releasePage() {
        dbPage = null;
    }


    /**
     * Read a sequence of bytes into the provided byte-array, starting with the
     * specified offset, and reading the specified number of bytes.
     *
     * @param b the byte-array to read bytes into
     *
     * @param off the offset to read the bytes into the array
     *
     * @param len the number of bytes to read into the array
     */
    public void read(byte[] b, int off, int len) throws IOException {
        checkDBPage();

        int pagePosition = getPositionPageOffset();
        
        if (pagePosition + len <= pageSize) {
            dbPage.read(pagePosition, b, off, len);
            position += len;
        }
        else {
            // Read part of the data from this page, then load the next page and
            // read the remainder of the data.
            int page1Len = pageSize - pagePosition;
            assert page1Len < len;
            dbPage.read(pagePosition, b, off, page1Len);

            // Load the second page and read the data.  The next page will be
            // loaded automatically since the position will move forward to the
            // first byte of the next page.
            position += page1Len;
            checkDBPage();
            dbPage.read(0, b, off + page1Len, len - page1Len);

            position += (len - page1Len);
        }
    }


    /**
     * Read a sequence of bytes into the provided byte-array.  The entire array
     * is filled from start to end.
     *
     * @param b the byte-array to read bytes into
     */
    public void read(byte[] b) throws IOException {
        read(b, 0, b.length);
    }


    /**
     * Reads and returns a Boolean value from the current position.  A zero
     * value is interpreted as <tt>false</tt>, and a nonzero value is
     * interpreted as <tt>true</tt>.
     */
    public boolean readBoolean() throws IOException {
        checkDBPage();
        boolean b = dbPage.readBoolean(getPositionPageOffset());
        position++;
        return b;
    }


    /** Reads and returns a signed byte from the current position. */
    public byte readByte() throws IOException {
        checkDBPage();
        byte b = dbPage.readByte(getPositionPageOffset());
        position++;
        return b;
    }

    /**
     * Reads and returns an unsigned byte from the current position.  The value
     * is returned as an <tt>int</tt> whose value will be between 0 and 255,
     * inclusive.
     */
    public int readUnsignedByte() throws IOException {
        checkDBPage();
        int b = dbPage.readUnsignedByte(getPositionPageOffset());
        position++;
        return b;
    }


    /**
     * Reads and returns an unsigned short from the current position.  The value
     * is returned as an <tt>int</tt> whose value will be between 0 and 65535,
     * inclusive.
     */
    public int readUnsignedShort() throws IOException {
        int pagePosition = getPositionPageOffset();

        int value;
        if (pagePosition + 2 <= pageSize) {
            checkDBPage();
            value = dbPage.readUnsignedShort(pagePosition);
            position += 2;
        }
        else {
            // Need to read the bytes spanning this page and the next.
            // Note that read() moves the file position forward.
            read(tmpBuf, 0, 2);
            value = ((tmpBuf[0] & 0xFF) << 8) | (tmpBuf[1] & 0xFF);
        }

        return value;
    }


    /** Reads and returns a signed short from the current position. */
    public short readShort() throws IOException {
        int pagePosition = getPositionPageOffset();

        short value;
        if (pagePosition + 2 <= pageSize) {
            checkDBPage();
            value = dbPage.readShort(pagePosition);
            position += 2;
        }
        else {
            // Need to read the bytes spanning this page and the next.
            // Note that read() moves the file position forward.
            read(tmpBuf, 0, 2);

            // Don't chop off high-order bits.  When byte is cast to int,
            // the sign will be extended, so if original byte is negative,
            // the resulting int will be too.
            value = (short) ((tmpBuf[0] <<  8) | (tmpBuf[1] & 0xFF));
        }

        return value;
    }


    /** Reads and returns a two-byte char value from the current position. */
    public char readChar() throws IOException {
        int pagePosition = getPositionPageOffset();

        char value;
        if (pagePosition + 2 <= pageSize) {
            checkDBPage();
            value = dbPage.readChar(pagePosition);
            position += 2;
        }
        else {
            // Need to read the bytes spanning this page and the next.
            // Note that read() moves the file position forward.
            read(tmpBuf, 0, 2);

            // Don't chop off high-order bits.  When byte is cast to int,
            // the sign will be extended, so if original byte is negative,
            // the resulting int will be too.
            value = (char) ((tmpBuf[0] <<  8) | (tmpBuf[1] & 0xFF));
        }

        return value;
    }


    /**
     * Reads and returns a four-byte unsigned integer value from the current
     * position.
     */
    public long readUnsignedInt() throws IOException {
        int pagePosition = getPositionPageOffset();

        long value;
        if (pagePosition + 4 <= pageSize) {
            checkDBPage();
            value = dbPage.readUnsignedInt(pagePosition);
            position += 4;
        }
        else {
            // Need to read the bytes spanning this page and the next.
            // Note that read() moves the file position forward.
            read(tmpBuf, 0, 4);
            value = ((tmpBuf[0] & 0xFF) << 24) | ((tmpBuf[1] & 0xFF) << 16) |
                    ((tmpBuf[2] & 0xFF) <<  8) | ((tmpBuf[3] & 0xFF)      );
        }

        return value;
    }


    /** Reads and returns a four-byte integer value from the current position. */
    public int readInt() throws IOException {
        int pagePosition = getPositionPageOffset();

        int value;
        if (pagePosition + 4 <= pageSize) {
            checkDBPage();
            value = dbPage.readInt(pagePosition);
            position += 4;
        }
        else {
            // Need to read the bytes spanning this page and the next.
            // Note that read() moves the file position forward.
            read(tmpBuf, 0, 4);
            value = ((tmpBuf[0] & 0xFF) << 24) | ((tmpBuf[1] & 0xFF) << 16) |
                    ((tmpBuf[2] & 0xFF) <<  8) | ((tmpBuf[3] & 0xFF)      );
        }

        return value;
    }

    /**
     * Reads and returns an eight-byte long integer value from the current
     * position.
     */
    public long readLong() throws IOException {
        int pagePosition = getPositionPageOffset();

        long value;
        if (pagePosition + 8 <= pageSize) {
            checkDBPage();
            value = dbPage.readLong(pagePosition);
            position += 8;
        }
        else {
            // Need to read the bytes spanning this page and the next.
            // Note that read() moves the file position forward.
            read(tmpBuf, 0, 8);
            value = ((long) (tmpBuf[0] & 0xFF) << 56) |
                    ((long) (tmpBuf[1] & 0xFF) << 48) |
                    ((long) (tmpBuf[2] & 0xFF) << 40) |
                    ((long) (tmpBuf[3] & 0xFF) << 32) |
                    ((long) (tmpBuf[4] & 0xFF) << 24) |
                    ((long) (tmpBuf[5] & 0xFF) << 16) |
                    ((long) (tmpBuf[6] & 0xFF) <<  8) |
                    ((long) (tmpBuf[7] & 0xFF)      );
        }

        return value;
    }

    public float readFloat() throws IOException {
        return Float.intBitsToFloat(readInt());
    }

    public double readDouble() throws IOException {
        return Double.longBitsToDouble(readLong());
    }


    /**
     * This method reads and returns a variable-length string whose maximum
     * length is 255 bytes.  The string is expected to be in US-ASCII encoding,
     * so multibyte characters are not supported.
     * <p>
     * The string's data format is expected to be a single unsigned byte
     * <em>b</em> specifying the string's length, followed by <em>b</em> more
     * bytes consisting of the string value itself.
     */
    public String readVarString255() throws IOException {
        int len = readUnsignedByte();
        byte[] strBytes = new byte[len];
        read(strBytes);

        String str = null;
        try {
            str = new String(strBytes, 0, len, "US-ASCII");
        }
        catch (UnsupportedEncodingException e) {
            // According to the Java docs, the US-ASCII character-encoding is
            // required to be supported by all JVMs.  So, this is not supposed
            // to happen.
            logger.error("The unthinkable has happened:  " + e);
        }

        return str;
    }


    /**
     * This method reads and returns a variable-length string whose maximum
     * length is 65535 bytes.  The string is expected to be in US-ASCII
     * encoding, so multibyte characters are not supported.
     * <p>
     * The string's data format is expected to be a single unsigned short (two
     * bytes) <em>s</em> specifying the string's length, followed by <em>s</em>
     * more bytes consisting of the string value itself.
     */
    public String readVarString65535() throws IOException {
        int len = readUnsignedShort();
        byte[] strBytes = new byte[len];
        read(strBytes);

        String str = null;
        try {
            str = new String(strBytes, 0, len, "US-ASCII");
        }
        catch (UnsupportedEncodingException e) {
            // According to the Java docs, the US-ASCII character-encoding is
            // required to be supported by all JVMs.  So, this is not supposed
            // to happen.
            logger.error("The unthinkable has happened:  " + e);
        }

        return str;
    }
}
