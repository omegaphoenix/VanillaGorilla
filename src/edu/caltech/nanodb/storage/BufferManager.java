package edu.caltech.nanodb.storage;


import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import edu.caltech.nanodb.client.SessionState;

import edu.caltech.nanodb.expressions.TypeCastException;

import edu.caltech.nanodb.server.NanoDBServer;
import edu.caltech.nanodb.server.properties.PropertyHandler;
import edu.caltech.nanodb.server.properties.ReadOnlyPropertyException;
import edu.caltech.nanodb.server.properties.UnrecognizedPropertyException;


/**
 * The buffer manager reduces the number of disk IO operations by managing an
 * in-memory cache of data pages.  It also imposes a limit on the maximum
 * amount of space that can be used for data pages in the database.
 *
 * @todo Eventually add integrity checks, e.g. to make sure every cached
 *       page's file appears in the collection of cached files.
 */
public class BufferManager {

    /**
     * The system property that can be used to specify the size of the page
     * cache in the buffer manager.
     */
    public static final String PROP_PAGECACHE_SIZE = "nanodb.pagecache.size";

    /** The default page-cache size is defined to be 20MB. */
    public static final long DEFAULT_PAGECACHE_SIZE = 20 * 1024 * 1024;


    /**
     * The system property that can be used to specify the page replacement
     * policy in the buffer manager.
     */
    public static final String PROP_PAGECACHE_POLICY = "nanodb.pagecache.policy";

    /** The default page-cache policy is LRU. */
    public static final String DEFAULT_PAGECACHE_POLICY = "lru";


    private static class DBPageID {
        private File file;

        private int pageNo;

        public DBPageID(File file, int pageNo) {
            this.file = file;
            this.pageNo = pageNo;
        }

        public DBPageID(DBPage dbPage) {
            this(dbPage.getDBFile().getDataFile(), dbPage.getPageNo());
        }

        @Override
        public boolean equals(Object obj) {
            if (obj instanceof DBPageID) {
                DBPageID other = (DBPageID) obj;
                return file.equals(other.file) && pageNo == other.pageNo;
            }
            return false;
        }

        @Override
        public int hashCode() {
            int hash = 17;
            hash = hash * 37 + file.hashCode();
            hash = hash * 37 + pageNo;
            return hash;
        }
    }


    /**
     * This helper class keeps track of a data page that is currently cached.
     */
    private static class CachedPageInfo {
        public DBFile dbFile;

        public int pageNo;

        public CachedPageInfo(DBFile dbFile, int pageNo) {
            if (dbFile == null)
                throw new IllegalArgumentException("dbFile cannot be null");

            this.dbFile = dbFile;
            this.pageNo = pageNo;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj instanceof CachedPageInfo) {
                CachedPageInfo other = (CachedPageInfo) obj;
                return dbFile.equals(other.dbFile) && pageNo == other.pageNo;
            }
            return false;
        }

        @Override
        public int hashCode() {
            int hash = 7;
            hash = 31 * hash + dbFile.hashCode();
            hash = 31 * hash + pageNo;
            return hash;
        }
    }


    /**
     * This helper class records the pin-count of a data page as imposed by a
     * given session, so that we can forcibly release the session's pins after
     * each command the session completes.
     */
    private static class SessionPinCount {
        /** The page that is pinned. */
        public DBPage dbPage;

        /** The number of times the session has pinned the page. */
        public int pinCount;

        public SessionPinCount(DBPage dbPage) {
            this.dbPage = dbPage;
            this.pinCount = 0;
        }
    }


    private class BufferManagerPropertyHandler implements PropertyHandler {

        @Override
        public Object getPropertyValue(String propertyName)
                throws UnrecognizedPropertyException {

            if (PROP_PAGECACHE_SIZE.equals(propertyName)) {
                return maxCacheSize;
            }
            else if (PROP_PAGECACHE_POLICY.equals(propertyName)) {
                return replacementPolicy;
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

            if (PROP_PAGECACHE_SIZE.equals(propertyName)) {
                throw new ReadOnlyPropertyException(propertyName +
                        " is read-only");
            }
            else if (PROP_PAGECACHE_POLICY.equals(propertyName)) {
                throw new ReadOnlyPropertyException(propertyName +
                        " is read-only");
            }
            else {
                throw new UnrecognizedPropertyException("No property named " +
                        propertyName);
            }
        }
    }


    /** A logging object for reporting anything interesting that happens. */
    private static Logger logger = Logger.getLogger(BufferManager.class);


    private FileManager fileManager;


    private ArrayList<BufferManagerObserver> observers;


    /**
     * This collection holds the {@link DBFile} objects corresponding to various
     * opened files the database is currently using.
     */
    private LinkedHashMap<String, DBFile> cachedFiles;


    /**
     * This collection holds database pages (not WAL pages) that the database
     * is currently working with, so that they don't continually need to be
     * reloaded.
     */
    private LinkedHashMap<CachedPageInfo, DBPage> cachedPages;


    /**
     * This collection maps session IDs to the files and pages that each
     * session has pinned, so that we can forcibly unpin pages used by a
     * given session when the session is done with the current command.
     */
    private HashMap<Integer, HashMap<DBPageID, SessionPinCount>> sessionPinCounts;


    /**
     * This set holds the identity hash-code of every buffer allocated by the
     * Buffer Manager, so that it can verify that buffers being freed are
     * actually from this Buffer Manager.
     *
     * @see System#identityHashCode
     */
    private HashSet<Integer> allocatedBuffers;


    /** This field records how many bytes are currently cached, in total. */
    private long totalBytesCached;


    /** This field records the maximum allowed cache size. */
    private long maxCacheSize;


    /**
     * A string indicating the buffer manager's page replacement policy.
     * Currently it can be "lru" or "fifo".
     */
    private String replacementPolicy;


    public BufferManager(NanoDBServer server, FileManager fileManager) {
        this.fileManager = fileManager;

        observers = new ArrayList<>();

        configureMaxCacheSize();

        cachedFiles = new LinkedHashMap<>();

        replacementPolicy = configureReplacementPolicy();
        cachedPages = new LinkedHashMap<>(16, 0.75f, "lru".equals(replacementPolicy));

        totalBytesCached = 0;
        allocatedBuffers = new HashSet<>();

        sessionPinCounts = new HashMap<>();

        if (server != null) {
            // Register properties that the Buffer Manager exposes.
            server.getPropertyRegistry().registerProperties(
                new BufferManagerPropertyHandler(),
                PROP_PAGECACHE_POLICY, PROP_PAGECACHE_SIZE);
        }
    }


    private void configureMaxCacheSize() {
        // Set the default up-front; it's just easier that way.
        maxCacheSize = DEFAULT_PAGECACHE_SIZE;

        String str = System.getProperty(PROP_PAGECACHE_SIZE);
        if (str != null) {
            str = str.trim().toLowerCase();

            long scale = 1;
            if (str.length() > 1) {
                char modifierChar = str.charAt(str.length() - 1);
                boolean removeModifier = true;
                if (modifierChar == 'k')
                    scale = 1024;
                else if (modifierChar == 'm')
                    scale = 1024 * 1024;
                else if (modifierChar == 'g')
                    scale = 1024 * 1024 * 1024;
                else
                    removeModifier = false;

                if (removeModifier)
                    str = str.substring(0, str.length() - 1);
            }

            try {
                maxCacheSize = Long.parseLong(str);
                maxCacheSize *= scale;
            }
            catch (NumberFormatException e) {
                logger.error(String.format(
                    "Could not parse page-cache size value \"%s\"; " +
                    "using default value of %d bytes",
                    System.getProperty(PROP_PAGECACHE_SIZE),
                    DEFAULT_PAGECACHE_SIZE));

                maxCacheSize = DEFAULT_PAGECACHE_SIZE;
            }
        }
    }


    private String configureReplacementPolicy() {
        String str = System.getProperty(PROP_PAGECACHE_POLICY,
            DEFAULT_PAGECACHE_POLICY);

        str = str.trim().toLowerCase();

        if (!("lru".equals(str) || "fifo".equals(str))) {
            logger.error(String.format(
                "Unrecognized value \"%s\" for page-cache replacement " +
                "policy; using default value of LRU.",
                System.getProperty(PROP_PAGECACHE_POLICY)));
        }

        return str;
    }


    /**
     * Add another observer to the buffer manager.
     *
     * @param obs the observer to add to the buffer manager
     */
    public void addObserver(BufferManagerObserver obs) {
        if (obs == null)
            throw new IllegalArgumentException("obs cannot be null");

        observers.add(obs);
    }


    /**
     * This method attempts to allocate a buffer of the specified size,
     * possibly evicting some existing buffers in order to make space.
     *
     * @param size the size of the buffer to allocate
     *
     * @return an array of bytes, of the specified size
     *
     * @throws IOException if a dirty page must be evicted from the buffer
     *         manager, and an IO error occurred while writing the page to
     *         persistent storage.
     */
    public byte[] allocBuffer(int size) throws IOException {
        if (size <= 0)
            throw new IllegalArgumentException("size must be > 0, got " + size);

        ensureSpaceAvailable(size);

        if (totalBytesCached + size > maxCacheSize) {
            throw new IllegalStateException(
                "Not enough room to allocate a buffer of " + size + " bytes!");
        }

        // Perform the allocation so that we know the JVM also has space...
        // Then update the total bytes in use by the buffer manager.
        byte[] buffer = new byte[size];
        totalBytesCached += size;

        // Record the identity of the buffer that we allocated, so that
        // releaseBuffer() can verify that it came from the buffer manager.
        // TODO:  System.identityHashCode() is not guaranteed to return a
        //        distinct value for every object, so we can have collisions
        //        on this value.  Come up with a different approach.
        // allocatedBuffers.add(System.identityHashCode(buffer));

        return buffer;
    }


    public void releaseBuffer(byte[] buffer) {
        // Verify that this was a buffer we allocated?
        // TODO:  System.identityHashCode() is not guaranteed to return a
        //        distinct value for every object, so we can have collisions
        //        on this value.  Come up with a different approach.
        /*
        if (!allocatedBuffers.remove(System.identityHashCode(buffer))) {
            throw new IllegalArgumentException("Received a buffer that " +
                "wasn't allocated by the Buffer Manager");
        }
        */

        // Record that the buffer's space is now available.
        totalBytesCached -= buffer.length;
    }


    /**
     * Retrieves the specified {@link DBFile} from the buffer manager, if it has
     * already been opened.
     *
     * @param filename The filename of the database file to retrieve.  This
     *        should be ONLY the database filename, no path.  The path is
     *        expected to be relative to the database's base directory.
     *
     * @return the {@link DBFile} corresponding to the filename, if it has
     *         already been opened, or <tt>null</tt> if the file isn't currently
     *         open.
     */
    public DBFile getFile(String filename) {
        DBFile dbFile = cachedFiles.get(filename);

        logger.debug(String.format(
            "Requested file %s is%s in file-cache.",
            filename, (dbFile != null ? "" : " NOT")));

        return dbFile;
    }


    public void addFile(DBFile dbFile) {
        if (dbFile == null)
            throw new IllegalArgumentException("dbFile cannot be null");

        String filename = dbFile.getDataFile().getName();
        if (cachedFiles.containsKey(filename)) {
            throw new IllegalStateException(
                "File cache already contains file " + filename);
        }

        // NOTE:  If we want to keep a cap on how many files are opened, we
        //        would do that here.

        logger.debug(String.format( "Adding file %s to file-cache.", filename));

        cachedFiles.put(filename, dbFile);
    }


    /**
     * Records that the page was pinned by the current session.  This method
     * does not actually pin the page; it is presumed that the page is already
     * pinned.
     *
     * @param dbPage the page that was pinned by the session
     */
    public void recordPagePinned(DBPage dbPage) {
        int sessionID = SessionState.get().getSessionID();

        // Retrieve the set of pages pinned by the current session.
        HashMap<DBPageID, SessionPinCount> pinnedBySession =
            sessionPinCounts.get(sessionID);
        if (pinnedBySession == null) {
            pinnedBySession = new HashMap<>();
            sessionPinCounts.put(sessionID, pinnedBySession);
        }

        // Find the session-specific pin-count for the data page.
        SessionPinCount spc = pinnedBySession.get(new DBPageID(dbPage));
        if (spc == null) {
            spc = new SessionPinCount(dbPage);
            pinnedBySession.put(new DBPageID(dbPage), spc);
        }

        // Finally, increment the session's pin-count on this page.
        spc.pinCount++;
    }


    /**
     * Records that the page was unpinned by the current session.  This method
     * does not actually unpin the page; it is presumed that the page will be
     * unpinned after this call.
     *
     * @param dbPage the page that was unpinned
     */
    public void recordPageUnpinned(DBPage dbPage) {
        int sessionID = SessionState.get().getSessionID();

        // Retrieve the set of pages pinned by the current session.
        HashMap<DBPageID, SessionPinCount> pinnedBySession =
            sessionPinCounts.get(sessionID);
        if (pinnedBySession == null) {
            logger.error(String.format("DBPage %d is being unpinned by " +
                "session %d, but we have no record of the session!",
                dbPage.getPageNo(), sessionID));
            return;
        }

        // Find the session-specific pin-count for the data page.
        DBPageID pageID = new DBPageID(dbPage);
        SessionPinCount spc = pinnedBySession.get(pageID);
        if (spc == null) {
            logger.error(String.format("DBPage %d is being unpinned by " +
                "session %d, but we have no record of it having been pinned!",
                dbPage.getPageNo(), sessionID));
            return;
        }

        // Record that the page was unpinned.
        spc.pinCount--;

        // If the pin-count went to zero, remove the SessionPinCount object.
        if (spc.pinCount == 0) {
            pinnedBySession.remove(pageID);

            // If the set of pages pinned by the current session is now empty,
            // remove the set of pages.
            if (pinnedBySession.isEmpty())
                sessionPinCounts.remove(sessionID);
        }
    }


    /**
     * This method unpins all pages pinned by the current session.  This is
     * generally done at the end of each transaction so that pages aren't
     * pinned forever, and can actually be evicted from the buffer manager.
     */
    public void unpinAllSessionPages() {
        // Unpin all pages pinned by this session.
        int sessionID = SessionState.get().getSessionID();

        // Retrieve the set of pages pinned by the current session.
        HashMap<DBPageID, SessionPinCount> pinnedBySession =
            sessionPinCounts.get(sessionID);

        if (pinnedBySession == null) {
            // Nothing to release!  Nice -- the session is very clean.
            return;
        }

        // Duplicate the collection's values so that we don't get concurrent
        // modification exceptions.
        ArrayList<SessionPinCount> spcs = new ArrayList<>();
        spcs.addAll(pinnedBySession.values());
        for (SessionPinCount spc : spcs) {
            // It would be an overstatement to say this is an error, since we
            // can recover from it.
            logger.warn(String.format("Session %d pinned %s %d times" +
                " without a corresponding unpin call", sessionID, spc.dbPage,
                spc.pinCount));

            while (spc.pinCount > 0)
                spc.dbPage.unpin();
        }

        // Since unpinning the pages calls back into the buffer manager,
        // we should automatically have all our "sessionPinCounts" state
        // cleaned up along the way.
    }


    public void recordPageInvalidated(DBPage dbPage) {
        if (dbPage == null)
            throw new IllegalArgumentException("dbPage cannot be null");

        int pageNo = dbPage.getPageNo();
        DBPageID pageID = new DBPageID(dbPage);
        if (dbPage.getPinCount() > 0) {
            logger.warn(String.format("DBPage %d is being invalidated, but " +
                "it has a pin-count of %d", pageNo, dbPage.getPinCount()));
        }

        for (int sessionID : sessionPinCounts.keySet()) {
            HashMap<DBPageID, SessionPinCount> pinnedBySession =
                sessionPinCounts.get(sessionID);

            SessionPinCount spc = pinnedBySession.remove(pageID);
            if (spc != null) {
                logger.warn(String.format("DBPage %d is being invalidated, " +
                    "but session %d has pinned it %d times", pageNo, sessionID,
                    spc.pinCount));
            }
        }
    }


    /**
     * Retrieves the specified {@code DBPage} from the Buffer Manager if it's
     * currently buffered, or {@code null} if the page is not currently
     * buffered.  If a page is returned, it is pinned before it is returned.
     *
     * @param dbFile the file containing the page to retrieve
     * @param pageNo the page number in the {@code DBFile} to retrieve
     * @return the requested {@code DBPage}, or {@code null} if not found
     */
    public DBPage getPage(DBFile dbFile, int pageNo) {
        DBPage dbPage = cachedPages.get(new CachedPageInfo(dbFile, pageNo));

        logger.debug(String.format(
            "Requested page [%s,%d] is%s in page-cache.",
            dbFile, pageNo, (dbPage != null ? "" : " NOT")));

        if (dbPage != null) {
            // Make sure this page is pinned by the session so that we don't
            // flush it until the session is done with it.
            dbPage.pin();
        }

        return dbPage;
    }


    /**
     * <p>
     * Adds a new, previously unbuffered {@code DBPage} to the Buffer Manager.
     * The page is pinned during the operation.
     * </p>
     * <p>
     * It is an error if the page is already in the Buffer Manager when this
     * method is called.
     * </p>
     *
     * @param dbPage the page to add to the Buffer Manager
     *
     * @throws IllegalStateException if the page already appears in the Buffer
     *         Manager.
     */
    public void addPage(DBPage dbPage) throws IOException {
        if (dbPage == null)
            throw new IllegalArgumentException("dbPage cannot be null");

        DBFile dbFile = dbPage.getDBFile();
        int pageNo = dbPage.getPageNo();

        CachedPageInfo cpi = new CachedPageInfo(dbFile, pageNo);
        if (cachedPages.containsKey(cpi)) {
            throw new IllegalStateException(String.format(
                "Page cache already contains page [%s,%d]", dbFile, pageNo));
        }

        logger.debug(String.format("Adding page [%s,%d] to page-cache.",
            dbFile, pageNo));

        // Make sure this page is pinned by the session so that we don't flush
        // it until the session is done with it.  We do that before adding it
        // to the cached-pages collection, so that another thread can't
        // reclaim the page out from under us.
        dbPage.pin();
        cachedPages.put(cpi, dbPage);
    }


    /**
     * This helper function ensures that the buffer manager has the specified
     * amount of space available.  This is done by removing pages out of the
     * buffer manager's cache
     *
     * @param bytesRequired the amount of space that should be made available
     *        in the cache, in bytes
     *
     * @throws IOException if an IO error occurs when flushing dirty pages out
     *         to disk
     */
    private void ensureSpaceAvailable(int bytesRequired) throws IOException {
        // If we already have enough space, return without doing anything.
        if (bytesRequired + totalBytesCached <= maxCacheSize)
            return;

        // We don't currently have enough space in the cache.  Try to solve
        // this problem by evicting pages.  We collect together the pages to
        // evict, so that we can update the write-ahead log before flushing
        // the pages.

        ArrayList<DBPage> dirtyPages = new ArrayList<>();

        if (!cachedPages.isEmpty()) {
            // The cache will be too large after adding this page.

            Iterator<Map.Entry<CachedPageInfo, DBPage>> entries =
                cachedPages.entrySet().iterator();

            while (entries.hasNext() &&
                bytesRequired + totalBytesCached > maxCacheSize) {
                Map.Entry<CachedPageInfo, DBPage> entry = entries.next();

                DBPage oldPage = entry.getValue();

                if (oldPage.isPinned())  // Can't flush pages that are in use.
                    continue;

                logger.debug(String.format(
                    "    Evicting page [%s,%d] from page-cache to make room.",
                    oldPage.getDBFile(), oldPage.getPageNo()));

                entries.remove();
                totalBytesCached -= oldPage.getPageSize();

                // If the page is dirty, we need to write its data to disk before
                // invalidating it.  Otherwise, just invalidate it.
                if (oldPage.isDirty()) {
                    logger.debug("    Evicted page is dirty; must save to disk.");
                    dirtyPages.add(oldPage);
                }
                else {
                    oldPage.invalidate();
                }
            }
        }

        // If we have any dirty data pages, they need to be flushed to disk.
        writeDirtyPages(dirtyPages, /* invalidate */ true);

        if (bytesRequired + totalBytesCached > maxCacheSize)
            logger.warn("Buffer manager is currently using too much space.");
    }


    /**
     * This helper method writes out a list of dirty pages from the buffer
     * manager, ensuring that if transactions are enabled, the
     * write-ahead-logging rule is satisfied.
     *
     * @param dirtyPages the list of dirty pages to write
     * @param invalidate if true then the dirty pages are invalidated so they
     *        must be reloaded from disk
     *
     * @throws IOException if an IO error occurs while flushing dirty pages
     */
    private void writeDirtyPages(List<DBPage> dirtyPages, boolean invalidate)
        throws IOException {

        if (!dirtyPages.isEmpty()) {
            // Pass the observers a read-only version of the pages so they
            // can't change things.
            List<DBPage> readOnlyPages =
                Collections.unmodifiableList(dirtyPages);

            for (BufferManagerObserver obs : observers)
                obs.beforeWriteDirtyPages(readOnlyPages);

            // Finally, we can write out each dirty page.
            for (DBPage dbPage : dirtyPages) {
                fileManager.savePage(dbPage.getDBFile(), dbPage.getPageNo(),
                                     dbPage.getPageData());

                dbPage.setDirty(false);

                if (invalidate)
                    dbPage.invalidate();
            }
        }
    }


    /**
     * This method writes all dirty pages in the specified file, optionally
     * syncing the file after performing the write.  The pages are not removed
     * from the buffer manager after writing them; their dirty state is simply
     * cleared.
     *
     * @param dbFile the file whose dirty pages should be written to disk
     *
     * @param minPageNo dirty pages with a page-number less than this value
     *        will not be written to disk
     *
     * @param maxPageNo dirty pages with a page-number greater than this value
     *        will not be written to disk
     *
     * @param sync If true then the database file will be sync'd to disk;
     *        if false then no sync will occur.  The sync will always occur,
     *        in case dirty pages had previously been flushed to disk without
     *        syncing.
     *
     * @throws IOException if an IO error occurs while updating the write-ahead
     *         log, or while writing the file's contents.
     */
    public void writeDBFile(DBFile dbFile, int minPageNo, int maxPageNo,
                            boolean sync) throws IOException {

        logger.info(String.format("Writing all dirty pages for file %s to disk%s.",
            dbFile, (sync ? " (with sync)" : "")));

        Iterator<Map.Entry<CachedPageInfo, DBPage>> entries =
            cachedPages.entrySet().iterator();

        ArrayList<DBPage> dirtyPages = new ArrayList<>();

        while (entries.hasNext()) {
            Map.Entry<CachedPageInfo, DBPage> entry = entries.next();

            CachedPageInfo info = entry.getKey();
            if (dbFile.equals(info.dbFile)) {
                DBPage oldPage = entry.getValue();
                if (!oldPage.isDirty())
                    continue;

                int pageNo = oldPage.getPageNo();
                if (pageNo < minPageNo || pageNo > maxPageNo)
                    continue;

                logger.debug(String.format("    Saving page [%s,%d] to disk.",
                    oldPage.getDBFile(), oldPage.getPageNo()));

                dirtyPages.add(oldPage);
            }
        }

        writeDirtyPages(dirtyPages, /* invalidate */ false);

        if (sync) {
            logger.debug("Syncing file " + dbFile);
            fileManager.syncDBFile(dbFile);
        }
    }


    /**
     * This method writes all dirty pages in the specified file, optionally
     * syncing the file after performing the write.  The pages are not removed
     * from the buffer manager after writing them; their dirty state is simply
     * cleared.
     *
     * @param dbFile the file whose dirty pages should be written to disk
     *
     * @param sync If true then the database file will be sync'd to disk;
     *        if false then no sync will occur.  The sync will always occur,
     *        in case dirty pages had previously been flushed to disk without
     *        syncing.
     *
     * @throws IOException if an IO error occurs while updating the write-ahead
     *         log, or while writing the file's contents.
     */
    public void writeDBFile(DBFile dbFile, boolean sync) throws IOException {
        writeDBFile(dbFile, 0, Integer.MAX_VALUE, sync);
    }


    /**
     * This method writes all dirty pages in the buffer manager to disk.  The
     * pages are not removed from the buffer manager after writing them; their
     * dirty state is simply cleared.
     *
     * @param sync if true, this method will sync all files in which dirty pages
     *        were found, with the exception of WAL files and the
     *        transaction-state file.  If false, no file syncing will be
     *        performed.
     *
     * @throws IOException if an IO error occurs while updating the write-ahead
     *         log, or while writing the file's contents.
     */
    public void writeAll(boolean sync) throws IOException {
        logger.info("Writing ALL dirty pages in the Buffer Manager to disk.");

        Iterator<Map.Entry<CachedPageInfo, DBPage>> entries =
            cachedPages.entrySet().iterator();

        ArrayList<DBPage> dirtyPages = new ArrayList<>();
        HashSet<DBFile> dirtyFiles = new HashSet<>();

        while (entries.hasNext()) {
            Map.Entry<CachedPageInfo, DBPage> entry = entries.next();

            DBPage oldPage = entry.getValue();
            if (!oldPage.isDirty())
                continue;

            DBFile dbFile = oldPage.getDBFile();
            DBFileType type = dbFile.getType();
            if (type != DBFileType.WRITE_AHEAD_LOG_FILE &&
                type != DBFileType.TXNSTATE_FILE) {
                dirtyFiles.add(oldPage.getDBFile());
            }

            logger.debug(String.format("    Saving page [%s,%d] to disk.",
                dbFile, oldPage.getPageNo()));

            dirtyPages.add(oldPage);
        }

        writeDirtyPages(dirtyPages, /* invalidate */ false);

        if (sync) {
            logger.debug("Synchronizing all files containing dirty pages to disk.");
            for (DBFile dbFile : dirtyFiles)
                fileManager.syncDBFile(dbFile);
        }
    }

    /**
     * This method removes all cached pages in the specified file from the
     * buffer manager, writing out any dirty pages in the process.  This method
     * is not generally recommended to be used, as it basically defeats the
     * purpose of the buffer manager in the first place; rather, the
     * {@link #writeDBFile} method should be used instead.  There is a specific
     * situation in which it is used, when a file is being removed from the
     * Buffer Manager by the Storage Manager.
     *
     * @param dbFile the file whose pages should be flushed from the cache
     *
     * @throws IOException if an IO error occurs while updating the write-ahead
     *         log, or the file's contents
     */
    public void flushDBFile(DBFile dbFile) throws IOException {
        logger.info("Flushing all pages for file " + dbFile +
            " from the Buffer Manager.");

        Iterator<Map.Entry<CachedPageInfo, DBPage>> entries =
            cachedPages.entrySet().iterator();

        ArrayList<DBPage> dirtyPages = new ArrayList<>();

        while (entries.hasNext()) {
            Map.Entry<CachedPageInfo, DBPage> entry = entries.next();

            CachedPageInfo info = entry.getKey();
            if (dbFile.equals(info.dbFile)) {
                DBPage oldPage = entry.getValue();

                logger.debug(String.format(
                    "    Evicting page [%s,%d] from page-cache.",
                    oldPage.getDBFile(), oldPage.getPageNo()));

                // Remove the page from the cache.
                entries.remove();
                totalBytesCached -= oldPage.getPageSize();

                // If the page is dirty, we need to write its data to disk before
                // invalidating it.  Otherwise, just invalidate it.
                if (oldPage.isDirty()) {
                    logger.debug("    Evicted page is dirty; must save to disk.");
                    dirtyPages.add(oldPage);
                }
                else {
                    oldPage.invalidate();
                }
            }
        }

        writeDirtyPages(dirtyPages, /* invalidate */ true);
    }


    /**
     * This method removes all cached pages from the buffer manager, writing
     * out any dirty pages in the process.  This method is not generally
     * recommended to be used, as it basically defeats the purpose of the
     * buffer manager in the first place; rather, the {@link #writeAll} method
     * should be used instead.  However, this method is useful to cause certain
     * performance issues to manifest with individual commands, and the Storage
     * Manager also uses it during shutdown processing to ensure all data is
     * saved to disk.
     *
     * @throws IOException if an IO error occurs while updating the write-ahead
     *         log, or the file's contents
     */
    public void flushAll() throws IOException {
        logger.info("Flushing ALL database pages from the Buffer Manager.");

        Iterator<Map.Entry<CachedPageInfo, DBPage>> entries =
            cachedPages.entrySet().iterator();

        ArrayList<DBPage> dirtyPages = new ArrayList<>();

        while (entries.hasNext()) {
            Map.Entry<CachedPageInfo, DBPage> entry = entries.next();

            DBPage oldPage = entry.getValue();

            logger.debug(String.format(
                "    Evicting page [%s,%d] from page-cache.",
                oldPage.getDBFile(), oldPage.getPageNo()));

            // Remove the page from the cache.
            entries.remove();
            totalBytesCached -= oldPage.getPageSize();

            // If the page is dirty, we need to write its data to disk before
            // invalidating it.  Otherwise, just invalidate it.
            if (oldPage.isDirty()) {
                logger.debug("    Evicted page is dirty; must save to disk.");
                dirtyPages.add(oldPage);
            }
            else {
                oldPage.invalidate();
            }
        }

        writeDirtyPages(dirtyPages, /* invalidate */ true);
    }


    /**
     * This method removes a file from the cache, first flushing all pages from
     * the file out of the cache.  This operation is used by the Storage Manager
     * to close a data file.
     *
     * @param dbFile the file to remove from the cache.
     *
     * @throws IOException if an IO error occurs while writing out dirty pages
     */
    public void removeDBFile(DBFile dbFile) throws IOException {
        logger.info("Removing DBFile " + dbFile + " from buffer manager");
        flushDBFile(dbFile);
        cachedFiles.remove(dbFile.getDataFile().getName());
    }


    /**
     * This method removes ALL files from the cache, first flushing all pages
     * from the cache so that any dirty pages will be saved to disk (possibly
     * updating the write-ahead log in the process).  This operation is used by
     * the Storage Manager during shutdown.
     *
     * @return a list of the files that were in the cache, so that they can be
     *         used by the caller if necessary (e.g. to sync and close each one)
     *
     * @throws IOException if an IO error occurs while writing out dirty pages
     */
    public List<DBFile> removeAll() throws IOException {
        logger.info("Removing ALL DBFiles from buffer manager");

        // Flush all pages, ensuring that dirty pages will be written too.
        flushAll();

        // Get the list of DBFiles we had in the cache, then clear the cache.
        ArrayList<DBFile> dbFiles = new ArrayList<>(cachedFiles.values());
        cachedFiles.clear();

        return dbFiles;
    }
}
