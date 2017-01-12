package edu.caltech.nanodb.storage;


import edu.caltech.nanodb.relations.ColumnType;

/**
 * This class facilitates sequences of read operations against a single
 * {@link DBPage} object, by providing "position" state that is also updated
 * after each read is performed.  All read operations call through to the
 * <tt>DBPage</tt>'s interface.
 *
 * @see PageWriter
 * @see java.io.DataInput
 */
public class PageReader {

    /** The page that the reader will read from. */
    protected DBPage dbPage;

    /** The current position in the page where reads will occur from. */
    protected int position;


    public PageReader(DBPage dbPage) {
        if (dbPage == null)
            throw new NullPointerException("dbPage");

        this.dbPage = dbPage;
        position = 0;
    }


    public DBPage getDBPage() {
        return dbPage;
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
        if (position < 0 || position >= dbPage.getPageSize()) {
            throw new IllegalArgumentException("position must be in range [0," +
                dbPage.getPageSize() + ") (got " + position + ")");
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
        if (position + n > dbPage.getPageSize())
            throw new IllegalArgumentException("can't move position past page end");
        else if (position + n < 0)
            throw new IllegalArgumentException("can't move position past page start");

        position += n;
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
    public void read(byte[] b, int off, int len) {
        dbPage.read(position, b, off, len);
        position += len;
    }


    /**
     * Read a sequence of bytes into the provided byte-array.  The entire array
     * is filled from start to end.
     *
     * @param b the byte-array to read bytes into
     */
    public void read(byte[] b) {
        read(b, 0, b.length);
    }


    /**
     * Reads and returns a Boolean value from the current position.  A zero
     * value is interpreted as <tt>false</tt>, and a nonzero value is
     * interpreted as <tt>true</tt>.
     */
    public boolean readBoolean() {
        return dbPage.readBoolean(position++);
    }

    /** Reads and returns a signed byte from the current position. */
    public byte readByte() {
        return dbPage.readByte(position++);
    }

    /**
     * Reads and returns an unsigned byte from the current position.  The value
     * is returned as an <tt>int</tt> whose value will be between 0 and 255,
     * inclusive.
     */
    public int readUnsignedByte() {
        return dbPage.readUnsignedByte(position++);
    }


    /**
     * Reads and returns an unsigned short from the current position.  The value
     * is returned as an <tt>int</tt> whose value will be between 0 and 65535,
     * inclusive.
     */
    public int readUnsignedShort() {
        int value = dbPage.readUnsignedShort(position);
        position += 2;

        return value;
    }


    /** Reads and returns a signed short from the current position. */
    public short readShort() {
        short value = dbPage.readShort(position);
        position += 2;

        return value;
    }


    /** Reads and returns a two-byte char value from the current position. */
    public char readChar() {
        char value = dbPage.readChar(position);
        position += 2;

        return value;
    }


    /**
     * Reads and returns a four-byte unsigned integer value from the current
     * position.
     */
    public long readUnsignedInt() {
        long value = dbPage.readUnsignedInt(position);
        position += 4;

        return value;
    }


    /** Reads and returns a four-byte integer value from the current position. */
    public int readInt() {
        int value = dbPage.readInt(position);
        position += 4;

        return value;
    }

    /**
     * Reads and returns an eight-byte long integer value from the current
     * position.
     */
    public long readLong() {
        long value = dbPage.readLong(position);
        position += 8;

        return value;
    }

    public float readFloat() {
        return Float.intBitsToFloat(readInt());
    }

    public double readDouble() {
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
    public String readVarString255() {
        String val = dbPage.readVarString255(position);
        position += (1 + val.length());
        return val;
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
    public String readVarString65535() {
        String val = dbPage.readVarString65535(position);
        position += (2 + val.length());
        return val;
    }


    /**
     * This method reads and returns a string whose length is fixed at a consant
     * size.  The string is expected to be in US-ASCII encoding, so multibyte
     * characters are not supported.
     * <p>
     * Shorter strings are padded with 0 bytes at the end of the string, and this
     * padding is removed when the string is read.  Thus, the actual string read
     * may be shorter than the specified length, but the number of bytes the
     * string's value takes in the page is exactly the specified length.
     */
    public String readFixedSizeString(int len) {
        String val = dbPage.readFixedSizeString(position, len);
        position += len;
        return val;
    }


    public Object readObject(ColumnType colType) {
        Object value = dbPage.readObject(position, colType);

        // Unfortunately, there's no easy way to get back the change in the
        // position, so we have to apply the position-changes here.
        switch (colType.getBaseType()) {

        case INTEGER:
        case FLOAT:
            position += 4;
            break;

        case SMALLINT:
            position += 2;
            break;

        case BIGINT:
        case DOUBLE:
            position += 8;
            break;

        case TINYINT:
            position += 1;
            break;

        case CHAR:
            position += colType.getLength();
            break;

        case VARCHAR:
            // TODO:  Assume it's always stored as a 64KB varchar, not 256B.
            position += 2 + ((String) value).length();
            break;

        default:
            throw new UnsupportedOperationException(
                "Cannot currently store type " + colType.getBaseType());
        }

        return value;
    }
}
