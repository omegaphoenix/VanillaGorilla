package edu.caltech.nanodb.storage;


import java.io.IOException;
import java.util.List;


/**
 * This interface allows other classes to respond to operations performed by
 * the {@link BufferManager}.  For example, the
 * {@link edu.caltech.nanodb.transactions.TransactionManager} observes the
 * {@code BufferManager} to update the write-ahead log files appropriately,
 * based on what pages are being written out to disk.
 */
public interface BufferManagerObserver {
    /**
     * This method is called before the buffer manager writes the specified
     * collection of pages.
     *
     * @param pages
     * @throws IOException
     */
    void beforeWriteDirtyPages(List<DBPage> pages) throws IOException;
}
