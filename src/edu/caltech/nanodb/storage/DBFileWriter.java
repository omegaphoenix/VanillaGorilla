package edu.caltech.nanodb.storage;


import java.io.IOException;
import java.io.UnsupportedEncodingException;

import org.apache.log4j.Logger;


/**
 */
public class DBFileWriter extends DBFileReader {
    /** A logging object for reporting anything interesting that happens. */
    private static Logger logger = Logger.getLogger(DBFileReader.class);


    public DBFileWriter(DBFile dbFile, StorageManager storageManager) {
        super(dbFile, storageManager);
        extendFile = true;
    }


    public void write(byte[] b, int off, int len) throws IOException {
        checkDBPage();

        int pagePosition = getPositionPageOffset();

        if (pagePosition + len <= pageSize) {
            dbPage.write(pagePosition, b, off, len);
            position += len;
        }
        else {
            // Write part of the data to this page, then load the next page and
            // write the remainder of the data.
            int page1Len = pageSize - pagePosition;
            assert page1Len < len;
            dbPage.write(pagePosition, b, off, page1Len);

            // Load the second page and write the data.  The next page will be
            // loaded automatically since the position will move forward to the
            // first byte of the next page.
            position += page1Len;
            checkDBPage();
            dbPage.write(0, b, off + page1Len, len - page1Len);

            position += (len - page1Len);
        }
    }


    public void write(byte[] b) throws IOException {
        // Use the version of write() with extra args.
        write(b, 0, b.length);
    }


    public void writeBoolean(boolean v) throws IOException {
        checkDBPage();
        dbPage.writeBoolean(getPositionPageOffset(), v);
        position++;
    }


    public void writeByte(int v) throws IOException {
        checkDBPage();
        dbPage.writeByte(getPositionPageOffset(), v);
        position++;
    }


    public void writeShort(int v) throws IOException {
        int pagePosition = getPositionPageOffset();

        if (pagePosition + 2 <= pageSize) {
            checkDBPage();
            dbPage.writeShort(pagePosition, v);
            position += 2;
        }
        else {
            // Need to write the bytes spanning this page and the next.
            tmpBuf[0] = (byte) (0xFF & (v >> 8));
            tmpBuf[1] = (byte) (0xFF &  v);

            // Note that write() moves the file position forward.
            write(tmpBuf, 0, 2);
        }
    }

    public void writeChar(int v) throws IOException {
        // Implementation is identical to writeShort()...
        writeShort(v);
    }


    public void writeInt(int v) throws IOException {
        int pagePosition = getPositionPageOffset();

        if (pagePosition + 4 <= pageSize) {
            checkDBPage();
            dbPage.writeInt(pagePosition, v);
            position += 4;
        }
        else {
            // Need to write the bytes spanning this page and the next.
            tmpBuf[0] = (byte) (0xFF & (v >> 24));
            tmpBuf[1] = (byte) (0xFF & (v >> 16));
            tmpBuf[2] = (byte) (0xFF & (v >>  8));
            tmpBuf[3] = (byte) (0xFF &  v);

            // Note that write() moves the file position forward.
            write(tmpBuf, 0, 4);
        }
    }


    public void writeLong(long v) throws IOException {
        int pagePosition = getPositionPageOffset();

        if (pagePosition + 8 <= pageSize) {
            checkDBPage();
            dbPage.writeLong(pagePosition, v);
            position += 8;
        }
        else {
            // Need to write the bytes spanning this page and the next.

            tmpBuf[0] = (byte) (0xFF & (v >> 56));
            tmpBuf[1] = (byte) (0xFF & (v >> 48));
            tmpBuf[2] = (byte) (0xFF & (v >> 40));
            tmpBuf[3] = (byte) (0xFF & (v >> 32));
            tmpBuf[4] = (byte) (0xFF & (v >> 24));
            tmpBuf[5] = (byte) (0xFF & (v >> 16));
            tmpBuf[6] = (byte) (0xFF & (v >>  8));
            tmpBuf[7] = (byte) (0xFF &  v);

            // Note that write() moves the file position forward.
            write(tmpBuf, 0, 8);
        }
    }


    public void writeFloat(float v) throws IOException {
        writeInt(Float.floatToIntBits(v));
    }


    public void writeDouble(double v) throws IOException {
        writeLong(Double.doubleToLongBits(v));
    }


    public void writeVarString255(String value) throws IOException {
        byte[] strBytes;

        try {
            strBytes = value.getBytes("US-ASCII");
        }
        catch (UnsupportedEncodingException e) {
            // According to the Java docs, the US-ASCII character-encoding is
            // required to be supported by all JVMs.  So, this is not supposed
            // to happen.
            logger.error("The unthinkable has happened!", e);
            throw new RuntimeException("The unthinkable has happened!", e);
        }

        if (strBytes.length > 255)
            throw new IllegalArgumentException("value must be 255 bytes or less");

        // These functions advance the position pointer.
        writeByte(strBytes.length);
        write(strBytes);
    }


    public void writeVarString65535(String value) throws IOException {
        byte[] strBytes;

        try {
            strBytes = value.getBytes("US-ASCII");
        }
        catch (UnsupportedEncodingException e) {
            // According to the Java docs, the US-ASCII character-encoding is
            // required to be supported by all JVMs.  So, this is not supposed
            // to happen.
            logger.error("The unthinkable has happened!", e);
            throw new RuntimeException("The unthinkable has happened!", e);
        }

        if (strBytes.length > 65535)
            throw new IllegalArgumentException("value must be 65535 bytes or less");

        // These functions advance the position pointer.
        writeShort(strBytes.length);
        write(strBytes);
    }
}
