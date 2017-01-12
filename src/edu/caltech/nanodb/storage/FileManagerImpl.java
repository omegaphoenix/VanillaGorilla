package edu.caltech.nanodb.storage;


import org.apache.log4j.Logger;

import java.io.EOFException;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;

import edu.caltech.nanodb.server.performance.PerformanceCounters;


/**
 * The File Manager provides unbuffered, low-level operations for working with
 * paged data files.  It really doesn't know anything about the internal file
 * formats of the data files, except that the first two bytes of the first
 * page must specify the type and page size for the data file.  (This is a
 * requirement of {@link #openDBFile}.)
 *
 * @design Although it might make more sense to put per-file operations like
 *         "load page" and "store page" on the {@link DBFile} class, we
 *         provide higher-level operations on the Storage Manager so that we
 *         can provide global buffering capabilities in one place.
 *
 * @design This class includes no multithreading support.  It maintains no
 *         internal state, so there isn't anything that needs to be guarded,
 *         but still, other classes using this class need to be careful to
 *         maintain proper multithreading.
 */
public class FileManagerImpl implements FileManager {

    /** A logging object for reporting anything interesting that happens. */
    private static Logger logger = Logger.getLogger(FileManagerImpl.class);


    /**
     * The threshold for what will count as a "large seek" within a single
     * file.  The units on this value is 512-byte sectors.  A value of 500 is
     * probably a bit low, since tracks on modern hard disks will almost
     * certainly contain over a hundred sectors.
     */
    public static final int LARGE_SEEK_THRESHOLD = 500;


    /**
     * The base directory that the file-manager should use for creating and
     * opening files.
     */
    private File baseDir;


    /**
     * The DBFile object of the file that was accessed most recently.  Used
     * for recording performance metrics regarding disk IOs.
     */
    private DBFile lastFileAccessed;


    /**
     * The page number of the file that was accessed most recently.  Used
     * for recording performance metrics regarding disk IOs.
     */
    private int lastPageNoAccessed;


    /**
     * Create a file-manager instance that uses the specified base directory.
     *
     * @param baseDir the base-directory that the file-manager should use
     */
    public FileManagerImpl(File baseDir) {
        if (baseDir == null)
            throw new IllegalArgumentException("baseDir cannot be null");

        if (!baseDir.isDirectory()) {
            throw new IllegalArgumentException("baseDir value " + baseDir +
               " is not a directory");
        }

        this.baseDir = baseDir;
    }

    @Override
    public File[] getDBFiles() {
        return this.baseDir.listFiles();
    }

    // Update our file-IO performance counters
    void updateFileIOPerfStats(DBFile dbFile, int pageNo, boolean read,
                               int bufSize) {
        if (lastFileAccessed == null || !dbFile.equals(lastFileAccessed)) {
            PerformanceCounters.inc(PerformanceCounters.STORAGE_FILE_CHANGES);
            lastPageNoAccessed = 0;
        }
        else {
            // Compute the "number of sectors difference" between the last
            // page we accessed and the current page we are accessing.  This
            // is obviously a guess, since we don't know the physical file
            // layout, or the physical sector size (it could be 4KiB too).
            long diff = dbFile.getPageSize() * (pageNo - lastPageNoAccessed);
            diff /= 512;

            PerformanceCounters.add(
                PerformanceCounters.STORAGE_FILE_DISTANCE_TRAVELED,
                Math.abs(diff));
        }

        PerformanceCounters.inc(read ? PerformanceCounters.STORAGE_PAGES_READ :
            PerformanceCounters.STORAGE_PAGES_WRITTEN);

        PerformanceCounters.add(read ? PerformanceCounters.STORAGE_BYTES_READ :
            PerformanceCounters.STORAGE_BYTES_WRITTEN, bufSize);

        lastFileAccessed = dbFile;
        lastPageNoAccessed = pageNo;
    }


    /**
     * This helper function calculates the file-position of the specified page.
     * Obviously, this value is dependent on the page size.
     *
     * @param dbFile the database file to compute the page-start for
     * @param pageNo the page number to access
     *
     * @return the offset of the specified page from the start of the database
     *         file
     *
     * @throws IllegalArgumentException if the page number is negative
     */
    private long getPageStart(DBFile dbFile, int pageNo) {
        if (pageNo < 0)
            throw new IllegalArgumentException("pageNo must be >= 0, got " + pageNo);

        long pageStart = pageNo;
        pageStart *= (long) dbFile.getPageSize();

        return pageStart;
    }


    @Override
    public boolean fileExists(String filename) {
        File f = new File(baseDir, filename);
        return f.exists();
    }


    @Override
    public DBFile createDBFile(String filename, DBFileType type, int pageSize)
        throws IOException {

        File f = new File(baseDir, filename);
        logger.debug("Creating new database file " + f + ".");
        if (!f.createNewFile())
            throw new IOException("File " + f + " already exists!");

        DBFile dbFile = new DBFile(f, type, pageSize);

        byte[] buffer = new byte[pageSize];
        buffer[0] = (byte) type.getID();
        buffer[1] = (byte) DBFile.encodePageSize(pageSize);

        savePage(dbFile, 0, buffer);

        return dbFile;
    }


    @Override
    public boolean renameDBFile(DBFile dbFile, String newFilename) {
        File dataFile = dbFile.getDataFile();
        File newDataFile = new File(baseDir, newFilename);
        if (dataFile.renameTo(newDataFile)) {
            // Rename succeeded!
            dbFile.setDataFile(newDataFile);
            return true;
        }

        // Rename failed.
        return false;
    }


    @Override
    public DBFile openDBFile(String filename) throws IOException {

        File f = new File(baseDir, filename);
        if (!f.isFile())
            throw new FileNotFoundException("File " + f + " doesn't exist.");

        RandomAccessFile fileContents = new RandomAccessFile(f, "rw");

        int typeID = fileContents.readUnsignedByte();
        int pageSize = DBFile.decodePageSize(fileContents.readUnsignedByte());

        DBFileType type = DBFileType.valueOf(typeID);
        if (type == null)
            throw new IOException("Unrecognized file type ID " + typeID);

        DBFile dbFile;
        try {
            dbFile = new DBFile(f, type, pageSize, fileContents);
        }
        catch (IllegalArgumentException iae) {
            throw new IOException("Invalid page size " + pageSize +
                " specified for data file " + f, iae);
        }

        logger.debug(String.format("Opened existing database file %s; " +
            "type is %s, page size is %d.", f, type, pageSize));

        return dbFile;
    }


    @Override
    public void loadPage(DBFile dbFile, int pageNo, byte[] buffer,
                         boolean create) throws IOException {

        if (pageNo < 0) {
            throw new IllegalArgumentException("pageNo must be >= 0, got " +
                pageNo);
        }

        if (buffer.length != dbFile.getPageSize()) {
            throw new IllegalArgumentException("Buffer has a different size" +
                " from the specified DBFile page-size");
        }

        // Update our file-IO performance counters
        updateFileIOPerfStats(dbFile, pageNo, /* read */ true, buffer.length);

        long pageStart = getPageStart(dbFile, pageNo);

        RandomAccessFile fileContents = dbFile.getFileContents();
        fileContents.seek(pageStart);
        try {
            fileContents.readFully(buffer);
        }
        catch (EOFException e) {
            if (create) {
                // Caller wants to create the page if it doesn't already exist
                // yet.  Don't let the exception propagate.

                logger.debug(String.format(
                    "Requested page %d doesn't yet exist in file %s; creating.",
                    pageNo, dbFile.getDataFile().getName()));

                // ...of course, we don't propagate the exception, but we also
                // don't actually extend the file's size until the page is
                // stored back to the file...
                long newLength = (1L + (long) pageNo) * (long) dbFile.getPageSize();

                // This check is just for safety.  It would be highly irregular
                // to get an EOF exception and then have the file actually be
                // longer than we expect.  But, if it happens, we'll scream.
                long oldLength = fileContents.length();
                if (oldLength < newLength) {
                    fileContents.setLength(newLength);
                    logger.debug("Set file " + dbFile + " length to " + newLength);
                }
                else {
                    String msg = "Expected DB file to be less than " +
                        newLength + " bytes long, but it's " + oldLength +
                        " bytes long!";

                    logger.error(msg);
                    throw new IOException(msg);
                }
            }
            else {
                // Caller expected the page to exist!  Let the exception propagate.
                throw e;
            }
        }
    }


    @Override
    public void loadPage(DBFile dbFile, int pageNo, byte[] buffer)
        throws IOException {
        loadPage(dbFile, pageNo, buffer, false);
    }


    @Override
    public void savePage(DBFile dbFile, int pageNo, byte[] buffer)
        throws IOException {

        if (pageNo < 0) {
            throw new IllegalArgumentException("pageNo must be >= 0, got " +
                pageNo);
        }

        if (buffer.length != dbFile.getPageSize()) {
            throw new IllegalArgumentException("Buffer has a different size" +
                " from the specified DBFile page-size");
        }

        // Update our file-IO performance counters
        updateFileIOPerfStats(dbFile, pageNo, /* read */ false, buffer.length);

        long pageStart = getPageStart(dbFile, pageNo);

        RandomAccessFile fileContents = dbFile.getFileContents();
        fileContents.seek(pageStart);
        fileContents.write(buffer);
    }


    @Override
    public void syncDBFile(DBFile dbFile) throws IOException {
        logger.info("Synchronizing database file to disk:  " + dbFile);
        dbFile.getFileContents().getFD().sync();
    }


    @Override
    public void closeDBFile(DBFile dbFile) throws IOException {
        // Sync the file before closing, so that we can have some confidence
        // that any modified data has reached the disk.
        syncDBFile(dbFile);

        logger.info("Closing database file:  " + dbFile);
        dbFile.getFileContents().close();
    }


    @Override
    public void deleteDBFile(String filename) throws IOException {

        File f = new File(baseDir, filename);
        deleteDBFile(f);
    }


    @Override
    public void deleteDBFile(File f) throws IOException {
        if (!f.delete())
            throw new IOException("Couldn't delete file \"" + f.getName() + "\".");
    }


    @Override
    public void deleteDBFile(DBFile dbFile) throws IOException {
        deleteDBFile(dbFile.getDataFile());
    }
}
