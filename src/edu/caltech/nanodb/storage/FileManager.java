package edu.caltech.nanodb.storage;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;


/**
 * This interface specifies all operations that file managers must provide.
 */
public interface FileManager {
    /**
     * Returns an array of the files in the database.
     * @return list of database files
     */
    File[] getDBFiles();

    /**
     * Returns true if the specified file exists, or false otherwise.
     *
     * @param filename the name of the file to check for existence
     *
     * @return true if the file exists, or false otherwise
     */
    boolean fileExists(String filename);

    /**
     * This method creates a new database file in the directory used by the
     * storage manager.  An exception is thrown if the file already exists.
     *
     * @param filename the name of the file to open to create the database file
     * @param type the type of database file being created
     * @param pageSize the page size to use when reading and writing the file
     *
     * @return a new database file object for the newly created file
     *
     * @throws IOException if the specified file already exists.
     * @throws IllegalArgumentException if the page size is not valid
     */
    DBFile createDBFile(String filename, DBFileType type, int pageSize)
        throws IOException;

    /**
     * Attempts to rename the specified {@link DBFile} to a new filename.
     * If successful, the {@code DBFile} object itself is updated with a new
     * {@link File} object reflecting the new name.  If failure, the
     * {@code DBFile} object is left untouched.
     *
     * @param dbFile the database file to rename
     *
     * @param newFilename the new name to give to the database file
     *
     * @return true if the rename succeeded, or false otherwise.
     */
    boolean renameDBFile(DBFile dbFile, String newFilename);

    /**
     * This method opens a database file, and reads in the file's type and page
     * size from the first two bytes of the first page.  The method uses the
     * {@link RandomAccessFile#readUnsignedShort} method to read the page
     * size from the data file when it is opened.
     *
     * @param filename the name of the database file to open
     * @return the successfully opened database file
     *
     * @throws FileNotFoundException if the specified file doesn't exist.
     * @throws IOException if a more general IO issue occurs.
     */
    DBFile openDBFile(String filename) throws IOException;

    /**
     * Loads a page from the underlying data file, and returns a new
     * {@link DBPage} object containing the data.  The <tt>create</tt> flag
     * controls whether an error is propagated, if the requested page is past
     * the end of the file.  (Note that if a new page is created, the file's
     * size will not reflect the new page until it is actually written to the
     * file.)
     * <p>
     * <em>This function does no page caching whatsoever.</em>  Requesting a
     * particular page multiple times will return multiple page objects, with
     * data loaded from the file each time.
     *
     * @param dbFile the database file to load the page from
     * @param pageNo the number of the page to load
     * @param create a flag specifying whether the page should be created if it
     *        doesn't already exist
     *
     * @throws IllegalArgumentException if the page number is negative, or if
     *         the buffer is not the same length as the file's page-size.
     *
     * @throws java.io.EOFException if the requested page is not in the data file,
     *         and the <tt>create</tt> flag is set to <tt>false</tt>.
     */
    void loadPage(DBFile dbFile, int pageNo, byte[] buffer,
                  boolean create) throws IOException;

    /**
     * Loads a page from the underlying data file, and returns a new
     * {@link DBPage} object containing the data.  This method always reports an
     * {@link java.io.EOFException} if the specified page is past the end of the
     * database file.
     * <p>
     * <em>This function does no page caching whatsoever.</em>  Requesting a
     * particular page multiple times will return multiple page objects, with
     * data loaded from the file each time.
     * <p>
     * (This method is simply a wrapper of
     * {@link #loadPage(DBFile, int, byte[], boolean)}, passing {@code false}
     * for {@code create}.)
     *
     * @param dbFile the database file to load the page from
     * @param pageNo the number of the page to load
     *
     * @throws IllegalArgumentException if the page number is negative, or if
     *         the buffer is not the same length as the file's page-size.
     *
     * @throws java.io.EOFException if the requested page is not in the data file.
     */
    void loadPage(DBFile dbFile, int pageNo, byte[] buffer)
        throws IOException;

    /**
     * Saves a page to the DB file, and then clears the page's dirty flag.
     * Note that the data might not actually be written to disk until a sync
     * operation is performed.
     *
     * @param dbFile the data file to write to
     * @param pageNo the page number to write the buffer to
     * @param buffer the data to write back to the page
     *
     * @throws IllegalArgumentException if the page number is negative, or if
     *         the buffer is not the same length as the file's page-size.
     *
     * @throws IOException if an error occurs while writing the page to disk
     */
    void savePage(DBFile dbFile, int pageNo, byte[] buffer)
        throws IOException;

    /**
     * This method ensures that all file-writes on the specified DB-file have
     * actually been synchronized to the disk.  Note that even after a call to
     * {@link #savePage}, the filesystem may postpone the write for various
     * reasons, or disks may also buffer the write operations in order to
     * optimize their storage to disk.  This method ensures that any buffered
     * writes will actually be written to the disk.
     *
     * @param dbFile the database file to synchronize
     *
     * @throws java.io.SyncFailedException if the synchronization operation
     *         cannot be guaranteed successful, or if it fails for some reason.
     *
     * @throws IOException if some other IO problem occurs.
     */
    void syncDBFile(DBFile dbFile) throws IOException;

    /**
     * Closes the underlying data file.  Obviously, subsequent read or write
     * attempts will fail after this method is called.
     *
     * @param dbFile the database file to close
     *
     * @throws IOException if the file cannot be closed for some reason.
     */
    void closeDBFile(DBFile dbFile) throws IOException;

    /**
     * Deletes the database file with the specified filename from the storage
     * manager's directory.
     *
     * @param filename the name of the file to delete
     *
     * @throws IOException if the file cannot be deleted for some reason.
     */
    void deleteDBFile(String filename) throws IOException;

    /**
     * Deletes the specified database file.
     *
     * @param f the file to delete
     *
     * @throws IOException if the file cannot be deleted for some reason.
     */
    void deleteDBFile(File f) throws IOException;

    /**
     * Deletes the specified database file.  The caller should ensure that the
     * database file is closed and is going to be unused.
     *
     * @param dbFile the database file to delete
     *
     * @throws IOException if the file cannot be deleted for some reason.
     */
    void deleteDBFile(DBFile dbFile) throws IOException;
}
