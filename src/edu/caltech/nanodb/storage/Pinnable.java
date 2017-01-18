package edu.caltech.nanodb.storage;


/**
 * <p>
 * This interface provides the basic "pin" and "unpin" operations that
 * pinnable objects need to provide.  An object is <em>pinnable</em> if it
 * uses a data buffer that is managed by the Buffer Manager.  An object's
 * pin-count is simply a reference-count indicating how many parts of the
 * database have the object "in use," where "in use" means that the code
 * intends to access the object's data buffer.
 * </p>
 * <p>
 * This pin-count is very important, because the Buffer Manager will sometimes
 * need to reclaim data buffers in order to stay within the "maximum memory
 * usage" limit of the database (see {@link BufferManager#maxCacheSize}).  The
 * Buffer Manager will only reclaim buffers that it knows are currently not in
 * use; in other words, buffers with a pin-count of 0.
 * </p>
 * <p>
 * (We are somewhat forced to do this manually by the fact that Java has no
 * destructors.  Other languages like C++, while introducing other annoying
 * responsibilities w.r.t. memory management, would make this easier, because
 * we could reliably use smart-pointers to keep track of whether someone is
 * using a tuple.  It would not require manual pinning/unpinning.)
 * </p>
 * <p>
 * Since the Buffer Manager manages {@link DBPage}s (each page will use one or
 * two buffers), these objects are pinnable.  Also,
 * {@link edu.caltech.nanodb.relations.Tuple tuples} are often backed by
 * {@code DBPage}s, so tuples are also pinnable.  To ensure that a tuple or
 * page's backing data buffers are not reclaimed when the Buffer Manager must
 * make space, the code using these objects should make sure that the object
 * remains pinned while its data buffers must remain in memory.
 * </p>
 *
 * <h2>Pinning and Unpinning {@code DBPage}s</h2>
 *
 * <p>
 * Anytime a data-page is fetched from the Storage Manager (e.g.
 * {@link StorageManager#loadDBPage(DBFile, int, boolean)}), its pin-count
 * will be incremented by 1 on behalf of the caller.  The first time a given
 * page is fetched, the pin-count will be 1, and as long as the page is
 * buffered, the pin-count will be incremented each time it is fetched again.
 * </p>
 * <p>
 * When a page's pin-count reaches 0, it means that the Buffer Manager is
 * allowed to evict the page and reclaim its buffers.  All other state in the
 * object will remain trustworthy (within the bounds of correct concurrent
 * access and modification, of course).  For example, an unpinned page will
 * still retain its page-number within the data file.  Such values can be
 * retrieved and used even on an unpinned page.
 * </p>
 * <p>
 * The intention of the design is that pages will only need to be manually
 * pinned and unpinned within the storage layer.  For example, {@link TupleFile}
 * implementations will likely need to manually pin and unpin pages, as will
 * {@link TupleFileManager} implementations.
 * </p>
 * <p>
 * The intended abstraction is that all operations within the query executor
 * code (e.g. plan nodes, the executor itself) will only need to worry about
 * unpinning tuples; they will not need to be aware of pinning or unpinning
 * the pages that back those tuples.
 * </p>
 *
 * <h2>Pinning and Unpinning {@code Tuple}s</h2>
 *
 * <p>
 * Anytime a tuple is retrieved from a tuple-file (e.g. many of the operations
 * specified in the {@link TupleFile} interface), the returned tuple will have
 * a pin-count of 1, and the backing {@code DBPage} will be incremented on
 * behalf of the tuple.  (This behavior is implemented in the
 * {@link PageTuple} constructor.)
 * </p>
 * <p>
 * Deleting a tuple does not unpin it.  The main reason why is so that the
 * query executor can simply unpin all tuples produced by the plan, regardless
 * of what the specific tuple-processor does to them.
 * </p>
 * <p>
 * When a tuple's pin-count reaches 0, it means that the Buffer Manager is
 * allowed to evict the tuple's backing page and reclaim the page's buffers.
 * All other state in the object will remain trustworthy (within the bounds of
 * correct concurrent access and modification, of course).  For example, an
 * unpinned tuple will still retain its location within the tuple file.  Such
 * values can be retrieved and used even on an unpinned tuple.
 * </p>
 */
public interface Pinnable {
    /**
     * Increase the pin-count on the object by one.  An object with a nonzero
     * pin-count cannot be released because it is in use.
     */
    public void pin();


    /**
     * Decrease the pin-count on the object by one.  When the pin-count
     * reaches zero, the object can be released.
     */
    public void unpin();


    /**
     * Returns the total number of times the object has been pinned.
     *
     * @return the total number of times the object has been pinned.
     */
    public int getPinCount();


    /**
     * Returns true if the object is currently pinned, false otherwise.
     */
    public boolean isPinned();
}
