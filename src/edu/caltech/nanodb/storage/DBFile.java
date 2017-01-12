package edu.caltech.nanodb.storage;


import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;


/**
 * <p>
 * This class provides page-level access to a database file, which contains
 * some kind of data utilized in a database system.  This class may be utilized
 * for many different kinds of database files.  Here is an example of the kinds
 * of data that might be stored in files:
 * </p>
 * <ul>
 *   <li>Tuples in a database table.</li>
 *   <li>Table indexes in a hashtable, tree, or some other format.</li>
 *   <li>Recovery logs.</li>
 *   <li>Checkpoint files.</li>
 * </ul>
 * <p>
 * <tt>DBFile</tt>s are created by using the {@link StorageManager#openDBFile}
 * method (or perhaps one of the wrapper methods such as
 * {@link StorageManager#openTable} or {@link StorageManager#openWALFile(int)}).
 * This allows the <tt>StorageManager</tt> to provide caching of opened
 * <tt>DBFile</tt>s so that unnecessary IOs can be avoided.  (Internally, the
 * <tt>StorageManager</tt> uses {@link FileManagerImpl#openDBFile} to open files,
 * and the {@link BufferManager} to cache opened files and loaded pages.)
 * </p>
 * <p>
 * For a file to be opened as a <tt>DBFile</tt>, it must have specific details
 * stored in the first page of the file:
 * </p>
 * <ul>
 *   <li><u>Byte 0:</u>  file type (unsigned byte) - value taken from
 *       {@link DBFileType}</li>
 *   <li><u>Byte 1:</u>  page size  <i>p</i> (unsigned byte) - file's page
 *       size is <i>P</i> = 2<sup>p</sup></li>
 * </ul>
 *
 * @see RandomAccessFile
 */
public class DBFile {
    /** The minimum page size is 512 bytes. */
    public static final int MIN_PAGESIZE = 512;

    /** The maximum page size is 64K bytes. */
    public static final int MAX_PAGESIZE = 65536;

    /** The default page size is 8K bytes. */
    public static final int DEFAULT_PAGESIZE = 8192;


    /** The actual data file on disk. */
    private File dataFile;


    /** The type of the data file. */
    private DBFileType type;


    /**
     * This is the size of pages that are read and written to the data file.
     * This value will be a power of two, between the minimum and maximum
     * page sizes.
     */
    private int pageSize;


    /** The file data is accessed via this variable. */
    private RandomAccessFile fileContents;


    /**
     * This static helper method returns true if the specified page size is
     * valid; i.e. it must be within the minimum and maximum page sizes, and
     * it must be a power of two.
     *
     * @param pageSize the page-size to test for validity
     *
     * @return true if the specified page-size is valid, or false otherwise.
     */
    public static boolean isValidPageSize(int pageSize) {
      // The first line ensures that the page size is in the proper range,
      // and the second line ensures that it is a power of two.
      return (pageSize >= MIN_PAGESIZE && pageSize <= MAX_PAGESIZE) &&
             ((pageSize & (pageSize - 1)) == 0);
    }


    /**
     * This static helper method checks the specified page-size with
     * {@link #isValidPageSize}, and if the size is not valid then an
     * {@link IllegalArgumentException} runtime exception is thrown.  The method
     * is intended for checking arguments.
     *
     * @param pageSize the page-size to test for validity
     *
     * @throws IllegalArgumentException if the specified page-size is invalid.
     */
    public static void checkValidPageSize(int pageSize) {
        if (!isValidPageSize(pageSize)) {
            throw new IllegalArgumentException(String.format(
                "Page size must be a power of two, in the range [%d, %d].  Got %d",
                MIN_PAGESIZE, MAX_PAGESIZE, pageSize));
        }
    }


    /**
     * Given a valid page size, this method returns the base-2 logarithm of the
     * page size for storing in a data file.  For example,
     * <tt>encodePageSize(512)</tt> will return 9.
     *
     * @param pageSize the page-size to encode
     *
     * @return the base-2 logarithm of the page size
     *
     * @throws IllegalArgumentException if the specified page-size is invalid.
     */
    public static int encodePageSize(int pageSize) {
        checkValidPageSize(pageSize);

        int encoded = 0;
        while (pageSize > 1) {
            pageSize >>= 1;
            encoded++;
        }

        return encoded;
    }


    /**
     * Given the base-2 logarithm of a page size, this method returns the actual
     * page size.  For example, <tt>decodePageSize(9)</tt> will return 512.
     *
     * @param encoded the encoded page-size
     *
     * @return the actual page size, computed as 2<sup><em>encoded</em></sup>.
     *
     * @throws IllegalArgumentException if the resulting page-size is invalid.
     */
    public static int decodePageSize(int encoded) {
        int pageSize = 0;

        if (encoded > 0)
            pageSize = 1 << encoded;

        checkValidPageSize(pageSize);

        return pageSize;
    }



    /**
     * Constructs a new object from the specified information, and opens the
     * backing data-file as well.
     *
     * @param dataFile the actual file containing the data
     * @param type the type of the data file
     * @param pageSize the page-size of the data file
     *
     * @throws IllegalArgumentException if the page size is not valid.
     * @throws IOException if some other IO error occurs
     */
    public DBFile(File dataFile, DBFileType type, int pageSize) throws IOException {
        this(dataFile, type, pageSize, new RandomAccessFile(dataFile, "rw"));
    }


    /**
     * Constructs a new object from the specified information and the previously
     * opened data-file.
     *
     * @param dataFile the actual file containing the data
     * @param type the type of the data file
     * @param pageSize the page-size of the data file
     * @param fileContents an already opened {@link RandomAccessFile} to use for
     *        accessing the data file's contents
     *
     * @throws IllegalArgumentException if the page size is not valid.
     * @throws IOException if some other IO error occurs
     */
    public DBFile(File dataFile, DBFileType type, int pageSize,
        RandomAccessFile fileContents) throws IOException {

        if (dataFile == null || type == null || fileContents == null)
            throw new NullPointerException();

        checkValidPageSize(pageSize);

        this.dataFile = dataFile;
        this.type = type;
        this.pageSize = pageSize;
        this.fileContents = fileContents;

        // Check to make sure the file contains a whole number of pages.
        long fileSize = fileContents.length();
        if (fileSize % (long) pageSize != 0) {
            // Maybe handle this someday by extending the file to have a whole
            // page at the end, but this is definitely the more conservative
            // approach.
            throw new IllegalStateException("Data file " + dataFile +
                " ends with a partial page!");
        }

        // TODO:  Verify that the file's stored page-size and type match the
        //        values we were passed!  (It's not that critical to verify,
        //        and generally these values are passed in directly from
        //        reading the datafile anyway.)
    }


    /**
     * Returns <tt>true</tt> if <tt>obj</tt> is an instance of <tt>DBFile</tt>
     * with the same backing file.
     */
    @Override
    public boolean equals(Object obj) {
        if (obj instanceof DBFile) {
            DBFile other = (DBFile) obj;
            return dataFile.equals(other.getDataFile());
        }

        return false;
    }


    /** Returns a hash-code based on the filename of the backing file. */
    @Override
    public int hashCode() {
      return dataFile.hashCode();
    }


    @Override
    public String toString() {
        return dataFile.getName();
    }


    /**
     * Returns the actual file that holds the data on the disk.
     *
     * @return the {@link File} object that actually holds the data on disk.
     */
    public File getDataFile() {
        return dataFile;
    }


    /**
     * Sets the actual file that holds the data on the disk.  <b>This should
     * only be called in special circumstances, like when a data file is
     * renamed.</b>
     */
    public void setDataFile(File file) {
        dataFile = file;
    }


    /**
     * Returns the type of this data file.
     *
     * @return the enumerated value indicating the file's type.
     */
    public DBFileType getType() {
        return type;
    }


    /**
     * Returns the page size for this database file, in bytes.
     *
     * @return the page size for this database file, in bytes.
     */
    public int getPageSize() {
        return pageSize;
    }


    /**
     * Reads the current file-length of this database file and computes the
     * total number of pages based on this value.  Note that since this method
     * involves an IO operation, it should be called infrequently since it will
     * be slow.
     *
     * @return the number of pages currently in this database file.
     *
     * @throws IOException if an IO error occurs while reading the file's length
     */
    public int getNumPages() throws IOException {
        long numPages = fileContents.length() / (long) pageSize;
        return (int) numPages;
    }


    /**
     * Returns the {@link RandomAccessFile} for accessing the data file's
     * contents.
     *
     * @return the {@link RandomAccessFile} for accessing the data file's
     *         contents.
     */
    public RandomAccessFile getFileContents() {
        return fileContents;
    }
}
