package edu.caltech.nanodb.storage;


import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;

import org.apache.log4j.Logger;

import edu.caltech.nanodb.expressions.TypeCastException;

import edu.caltech.nanodb.indexes.BasicIndexManager;
import edu.caltech.nanodb.indexes.IndexManager;

import edu.caltech.nanodb.server.EventDispatcher;
import edu.caltech.nanodb.server.NanoDBServer;
import edu.caltech.nanodb.server.properties.PropertyHandler;
import edu.caltech.nanodb.server.properties.ReadOnlyPropertyException;
import edu.caltech.nanodb.server.properties.UnrecognizedPropertyException;

import edu.caltech.nanodb.storage.heapfile.HeapTupleFileManager;
import edu.caltech.nanodb.transactions.TransactionManager;


/**
 *
 *
 * @todo This class requires synchronization, once we support multiple clients.
 */
public class StorageManager {

    /** A logging object for reporting anything interesting that happens. */
    private static Logger logger = Logger.getLogger(StorageManager.class);


    /*========================================================================
     * STATIC FIELDS AND METHODS
     */


    public static final boolean ENABLE_TRANSACTIONS = false;


    public static final boolean ENABLE_INDEXES = true;


    /**
     * The system property that can be used to specify the base-directory path
     * for the storage manager to use.
     */
    public static final String PROP_BASEDIR = "nanodb.basedir";


    /**
     * The system property that can be used to specify the default page-size
     * to use when creating new database files.
     */
    public static final String PROP_PAGESIZE = "nanodb.pagesize";


    /**
     * The default base-directory path used by the storage manager.  This
     * value is set to "<tt>./datafiles</tt>" (or "<tt>.\datafiles</tt>"
     * if on Windows).
     */
    public static final String DEFAULT_BASEDIR_PATH =
        "." + File.separator + "datafiles";


    /**
     * Returns the current page size to use for new database files.  If the
     * <tt>nanodb.pagesize</tt> system property is a valid page size then this
     * value is used.  Otherwise, the {@link DBFile#DEFAULT_PAGESIZE} value is
     * used.
     *
     * @return the current page size to use for new database files
     */
    public static int getCurrentPageSize() {
        // Use the default page size if no property value is specified.
        int pageSize = DBFile.DEFAULT_PAGESIZE;

        String pageSizeStr = System.getProperty(PROP_PAGESIZE);
        if (pageSizeStr != null) {
            try {
                pageSize = Integer.parseInt(pageSizeStr);
            }
            catch (NumberFormatException nfe) {
                logger.warn("Current value of " + PROP_PAGESIZE +
                    " property is not an integer:  \"" + pageSizeStr + "\"");
            }

            if (!DBFile.isValidPageSize(pageSize)) {
                logger.warn("Current value of " + PROP_PAGESIZE +
                    " property is not a valid page size:  " + pageSize);

                pageSize = DBFile.DEFAULT_PAGESIZE;
            }
        }

        return pageSize;
    }


    private class StoragePropertyHandler implements PropertyHandler {

        @Override
        public Object getPropertyValue(String propertyName)
            throws UnrecognizedPropertyException {

            if (PROP_PAGESIZE.equals(propertyName)) {
                return getCurrentPageSize();
            }
            else if (PROP_BASEDIR.equals(propertyName)) {
                return getBaseDir().toString();
            }
            else {
                throw new UnrecognizedPropertyException("No property named " +
                    propertyName);
            }
        }

        @Override
        public void setPropertyValue(String propertyName, Object value)
            throws UnrecognizedPropertyException, ReadOnlyPropertyException,
                   TypeCastException {

            if (PROP_PAGESIZE.equals(propertyName)) {
                throw new ReadOnlyPropertyException(propertyName +
                    " is read-only");
            }
            else if (PROP_BASEDIR.equals(propertyName)) {
                throw new ReadOnlyPropertyException(propertyName +
                        " is read-only");
            }
            else {
                throw new UnrecognizedPropertyException("No property named " +
                    propertyName);
            }
        }
    }


    /*========================================================================
     * NON-STATIC FIELDS AND METHODS
     */


    /**
     * The database server that this storage manager is a part of.  It
     * provides a number of the components that the storage manager depends
     * on, such as the event dispatcher and property registry.
     */
    private NanoDBServer server;


    /** The base directory, in which all database files are stored. */
    private File baseDir;


    /**
     * A flag recording whether the Storage Manager instance has been
     * initialized.
     */
    private boolean initialized = false;


    /** The buffer manager stores data pages in memory, to avoid disk IOs. */
    private BufferManager bufferManager;


    /**
     * The file manager performs basic operations against the filesystem,
     * without performing any buffering whatsoever.
     */
    private FileManager fileManager;


    /**
     * If transactions are enabled, this will be the singleton transaction
     * manager instance; otherwise, it will be {@code null}.
     */
    private TransactionManager transactionManager;


    private TableManager tableManager;


    private IndexManager indexManager;


    /**
     * This mapping is used to keep track of the tuple-file managers for all
     * the kinds of tuple-files we support.
     */
    private HashMap<DBFileType, TupleFileManager> tupleFileManagers =
        new HashMap<>();


    /**
     * This method initializes the storage manager.  It should only be called
     * once.
     *
     * @throws IllegalStateException if <tt>init()</tt> has already been called
     * @throws IOException if the storage manager cannot access the data
     *         directory for some reason
     */
    public void initialize(NanoDBServer server, File baseDir) throws IOException {
        if (initialized) {
            throw new IllegalStateException(
                "Storage manager is already initialized.");
        }

        if (server == null)
            throw new IllegalArgumentException("server cannot be null");

        this.server = server;

        // Make sure the base directory exists and is valid and all that.

        if (baseDir == null) {
            String defaultBaseDir = System.getProperty(PROP_BASEDIR,
                DEFAULT_BASEDIR_PATH);

            baseDir = new File(defaultBaseDir);
        }

        if (!baseDir.exists()) {
            logger.info("Base directory " + baseDir + " doesn't exist; creating.");
            if (!baseDir.mkdirs()) {
                throw new IOException("Couldn't create base directory " + baseDir);
            }
        }

        if (!baseDir.isDirectory()) {
            throw new IOException("Base-directory path " + baseDir +
                " doesn't refer to a directory.");
        }

        logger.info("Using base directory " + baseDir);
        this.baseDir = baseDir;

        // Register properties that the Storage Manager exposes.
        server.getPropertyRegistry().registerProperties(
            new StoragePropertyHandler(), PROP_PAGESIZE, PROP_BASEDIR);

        fileManager = new FileManagerImpl(baseDir);
        bufferManager = new BufferManager(server, fileManager);

        tupleFileManagers.put(DBFileType.HEAP_TUPLE_FILE,
            new HeapTupleFileManager(this));

        // TODO:  Register B-tree file-type here.

        if (TransactionManager.isEnabled()) {
            logger.info("Initializing transaction manager.");
            transactionManager = new TransactionManager(server);

            // This method opens the transaction-state file, performs any
            // necessary recovery operations, and so forth.
            transactionManager.initialize();
        }
        else {
            logger.info("Transaction manager is disabled.");
        }

        tableManager = new IndexedTableManager(this);
        indexManager = new BasicIndexManager(this);

        if (ENABLE_INDEXES) {
            EventDispatcher eventDispatcher = server.getEventDispatcher();

            // TODO:  Register the event-handler that enforces database constraints!
            // eventDispatcher.addRowEventListener(new DatabaseConstraintEnforcer(this));

            // TODO:  Register the event-handler that updates indexes when tables change.
            // eventDispatcher.addRowEventListener(new IndexUpdater(this));
        }

        initialized = true;
    }


    /**
     * This method shuts down the storage manager.  It should only be called
     * once.
     *
     * @throws IllegalStateException if <tt>init()</tt> has not been called
     * @throws IOException if the storage manager cannot save all data for some
     *         reason
     */
    public void shutdown() throws IOException {
        // Detect if already shut down...
        if (!initialized) {
            throw new IllegalStateException(
                "Storage manager is not initialized.");
        }

        if (transactionManager != null)
            transactionManager.forceWAL();

        List<DBFile> dbFiles = bufferManager.removeAll();
        for (DBFile dbFile : dbFiles)
            fileManager.closeDBFile(dbFile);

        // Register properties that the Storage Manager exposes.
        server.getPropertyRegistry().unregisterProperties(
            PROP_PAGESIZE, PROP_BASEDIR);

        initialized = false;
    }


    /**
     * Returns the base directory where all database files are stored.
     *
     * @return the base directory where all database files are stored
     */
    public File getBaseDir() {
        return baseDir;
    }


    public FileManager getFileManager() {
        return fileManager;
    }


    public BufferManager getBufferManager() {
        return bufferManager;
    }


    public TransactionManager getTransactionManager() {
        return transactionManager;
    }


    public DBFile createDBFile(String filename, DBFileType type)
        throws IOException {

        if (bufferManager.getFile(filename) != null) {
            throw new IllegalStateException("A file " + filename +
                " is already cached in the Buffer Manager!  Does it already exist?");
        }

        DBFile dbFile = fileManager.createDBFile(filename, type,
            getCurrentPageSize());

        bufferManager.addFile(dbFile);

        return dbFile;
    }


    public DBFile openDBFile(String filename) throws IOException {
        DBFile dbFile = bufferManager.getFile(filename);
        if (dbFile == null) {
            dbFile = fileManager.openDBFile(filename);
            bufferManager.addFile(dbFile);
        }

        return dbFile;
    }


    public TupleFile openTupleFile(String filename) throws IOException {
        DBFile dbFile = fileManager.openDBFile(filename);
        DBFileType type = dbFile.getType();
        TupleFileManager tfManager = getTupleFileManager(type);

        logger.debug(String.format("Opened DBFile for tuple file at path %s.",
            dbFile.getDataFile()));
        logger.debug(String.format("Type is %s, page size is %d bytes.",
            type, dbFile.getPageSize()));

        return tfManager.openTupleFile(dbFile);
    }


    private void closeDBFile(DBFile dbFile) throws IOException {
        bufferManager.removeDBFile(dbFile);
        fileManager.closeDBFile(dbFile);
    }


    public TableManager getTableManager() {
        return tableManager;
    }


    public IndexManager getIndexManager() {
        return indexManager;
    }


    /**
     * Returns the tuple-file manager for the specified file type.
     *
     * @param type the database file type to get the tuple-file manager for.
     *
     * @return the tuple-file manager instance for the specified file type
     *
     * @throws IllegalArgumentException if the file-type is <tt>null</tt>, or
     *         if the file-type is currently unsupported.
     */
    public TupleFileManager getTupleFileManager(DBFileType type) {
        if (type == null)
            throw new IllegalArgumentException("type cannot be null");

        TupleFileManager manager = tupleFileManagers.get(type);
        if (manager == null) {
            throw new IllegalArgumentException(
                "Unsupported tuple-file type:  " + type);
        }

        return manager;
    }


    /**
     * This method returns a database page to use, retrieving it from the buffer
     * manager if it is already loaded, or reading it from the specified data
     * file if it is not already loaded.  If the page must be loaded from the
     * file, it will be added to the buffer manager.  This operation may cause
     * other database pages to be evicted from the buffer manager, and written
     * back to disk if the evicted pages are dirty.
     * <p>
     * The <tt>create</tt> flag controls whether an error is propagated, if the
     * requested page is past the current end of the data file.  (Note that if a
     * new page is created, the file's size will not reflect the new page until
     * it is actually written to the file.)
     *
     * @param dbFile the database file to load the page from
     * @param pageNo the number of the page to load
     * @param create a flag specifying whether the page should be created if it
     *        doesn't already exist
     *
     * @return the database page, either from cache or from the data file
     *
     * @throws IllegalArgumentException if the page number is negative
     *
     * @throws java.io.EOFException if the requested page is not in the data file,
     *         and the <tt>create</tt> flag is set to <tt>false</tt>.
     */
    public DBPage loadDBPage(DBFile dbFile, int pageNo, boolean create)
        throws IOException {

        // Try to retrieve from the buffer manager.
        DBPage dbPage = bufferManager.getPage(dbFile, pageNo);
        if (dbPage == null) {
            // Buffer manager didn't have it.  Read the page directly from
            // the file, then add it to the buffer manager.
            dbPage = new DBPage(bufferManager, dbFile, pageNo);
            try {
                fileManager.loadPage(dbFile, pageNo, dbPage.getPageData(), create);
                bufferManager.addPage(dbPage);
            }
            catch (IOException e) {
                // Make sure to release the DBPage's buffer, or else we will
                // have a resource leak...
                dbPage.invalidate();
                throw e;
            }
        }

        return dbPage;
    }


    /**
     * This method returns a database page to use, retrieving it from the buffer
     * manager if it is already loaded, or reading it from the specified data
     * file if it is not already loaded.  If the page must be loaded from the
     * file, it will be added to the buffer manager.  This operation may cause
     * other database pages to be evicted from the buffer manager, and written
     * back to disk if the evicted pages are dirty.
     * <p>
     * (This method is simply a wrapper of
     * {@link #loadDBPage(DBFile, int, boolean)}, passing <tt>false</tt> for
     * <tt>create</tt>.)
     *
     * @param dbFile the database file to load the page from
     * @param pageNo the number of the page to load
     *
     * @return the database page, either from cache or from the data file
     *
     * @throws IllegalArgumentException if the page number is negative
     *
     * @throws java.io.EOFException if the requested page is not in the data file,
     *         and the <tt>create</tt> flag is set to <tt>false</tt>.
     */
    public DBPage loadDBPage(DBFile dbFile, int pageNo) throws IOException {
        return loadDBPage(dbFile, pageNo, false);
    }


    /**
     * This method causes any changes to the specified page to be logged by
     * the transaction manager's write-ahead log, so that the changes can be
     * redone or undone as may be appropriate.  Once the page's changes have
     * been logged, the {@link DBPage#syncOldPageData} method is called on
     * the page, since the page's changes have been recorded in the WAL.
     *
     * @param dbPage the page to record changes for
     */
    public void logDBPageWrite(DBPage dbPage) throws IOException {
        // If the page is dirty, record its changes to the write-ahead log.
        if (transactionManager != null)
            transactionManager.recordPageUpdate(dbPage);
    }


    /**
     * This method allows all data to be flushed from the Buffer Manager.  It
     * should not be used in practice, but it is useful to remove buffering to
     * expose performance issues in the storage layer.
     *
     * @see BufferManager#flushAll
     *
     * @throws IOException
     */
    public void flushAllData() throws IOException {
        bufferManager.flushAll();
    }
}
