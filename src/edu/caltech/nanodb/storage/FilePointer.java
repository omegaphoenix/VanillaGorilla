package edu.caltech.nanodb.storage;


/**
 * This class represents a pointer to a location within a database file.
 * Because database files are broken into pages, the pointer contains the
 * (zero-based) page number of the data, and the (zero-based) offset of the data
 * within the page.
 * <p>
 * File pointers themselves can be stored into database files for various
 * purposes.  The information is stored as follows:
 * <ul>
 *   <li>The page number is stored as a signed 32-bit value.</li>
 *   <li>The offset is stored as an unsigned 16-bit value.  (Note that database
 *       files are limited to a maximum page-size of 64 Kbytes.)</li>
 * </ul>
 */
public class FilePointer implements Comparable<FilePointer>, Cloneable {

    public static final FilePointer ZERO_FILE_POINTER = new FilePointer(0, 0);


    /** The page number in the table file.  This value is nonnegative. */
    private int pageNo;

    /** The offset of the data within the page.  This value is nonnegative. */
    private int offset;


    /** Construct a new file pointer. */
    public FilePointer(int pageNo, int offset) {
        if (pageNo < 0) {
            throw new IllegalArgumentException("pageNo must be >= 0 (got " +
                pageNo + ")");
        }

        if (offset < 0) {
            throw new IllegalArgumentException("offset must be >= 0 (got " +
                offset + ")");
        }

        this.pageNo = pageNo;
        this.offset = offset;
    }


    /** Returns the page number that the data is in. */
    public int getPageNo() {
        return pageNo;
    }


    /** Returns the offset within the page where the data starts. */
    public int getOffset() {
        return offset;
    }


    @Override
    public String toString() {
        return String.format("FP[%d:%d]", pageNo, offset);
    }


    /**
     * Returns <em>true</em> if <code>obj</code> refers to a
     * <code>FilePointer</code> with the same internal values.
     */
    @Override
    public boolean equals(Object obj) {
        if (obj instanceof FilePointer) {
            FilePointer fp = (FilePointer) obj;
            return pageNo == fp.pageNo && offset == fp.offset;
        }
        return false;
    }


    /** Calculate a hash-code for the file pointer. */
    @Override
    public int hashCode() {
        int hashCode;

        // Follows pattern in Effective Java, Item 8, with different constants.
        hashCode = 23;
        hashCode = 41 * hashCode + pageNo;
        hashCode = 41 * hashCode + offset;

        return hashCode;
    }


    @Override
    public int compareTo(FilePointer filePointer) {
        int compareResult = pageNo - filePointer.getPageNo();

        if (compareResult == 0)
            compareResult = offset - filePointer.getOffset();

        return compareResult;
    }


    @Override
    public Object clone() throws CloneNotSupportedException {
        return super.clone();
    }
}
