package edu.caltech.nanodb.storage;


import edu.caltech.nanodb.relations.ColumnType;


/**
 * This class extends the {@link PageReader} class to provide write operations
 * as well as read operations.  Using any of the write operations will
 * automatically set the page's dirty-flag to <tt>true</tt>.
 */
public class PageWriter extends PageReader
{
    public PageWriter(DBPage dbPage) {
        super(dbPage);
    }


    public void write(byte[] b) {
        // Use the version of write() with extra args.
        write(b, 0, b.length);
    }


    public void write(byte[] b, int off, int len) {
        dbPage.write(position, b, off, len);
        position += len;
    }


    public void writeBoolean(boolean v) {
        dbPage.writeBoolean(position++, v);
    }


    public void writeByte(int v) {
        dbPage.writeByte(position++, v);
    }


    public void writeShort(int v) {
        dbPage.writeShort(position, v);
        position += 2;
    }

    public void writeChar(int v) {
        // Implementation is identical to writeShort()...
        writeShort(v);
    }


    public void writeInt(int v) {
        dbPage.writeInt(position, v);
        position += 4;
    }


    public void writeLong(long v) {
        dbPage.writeLong(position, v);
        position += 8;
    }


    public void writeFloat(float v) {
        writeInt(Float.floatToIntBits(v));
    }


    public void writeDouble(double v) {
        writeLong(Double.doubleToLongBits(v));
    }


    public void writeVarString255(String value) {
        dbPage.writeVarString255(position, value);
        position += 1 + value.length();
    }


    public void writeVarString65535(String value) {
        dbPage.writeVarString65535(position, value);
        position += 2 + value.length();
    }


    public void writeFixedSizeString(String value, int len) {
        dbPage.writeFixedSizeString(position, value, len);
        position += len;
    }


    public void writeObject(ColumnType colType, Object value) {
        position += dbPage.writeObject(position, colType, value);
    }
}
