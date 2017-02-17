package edu.caltech.nanodb.storage.btreefile;


import java.io.IOException;

import java.util.List;

import edu.caltech.nanodb.relations.Schema;
import org.apache.log4j.Logger;

import edu.caltech.nanodb.expressions.TupleLiteral;
import edu.caltech.nanodb.relations.Tuple;
import edu.caltech.nanodb.storage.DBPage;
import edu.caltech.nanodb.storage.PageTuple;

import static edu.caltech.nanodb.storage.btreefile.BTreePageTypes.*;


/**
 * <p>
 * This class wraps a {@link DBPage} object that is an inner page in the
 * B<sup>+</sup> tree file implementation, to provide some of the basic
 * inner-page-management operations necessary for the file structure.
 * </p>
 * <p>
 * Operations involving individual leaf-pages are provided by the
 * {@link LeafPage} wrapper-class.  Higher-level operations involving multiple
 * leaves and/or inner pages of the B<sup>+</sup> tree structure, are provided
 * by the {@link LeafPageOperations} and {@link InnerPageOperations} classes.
 * </p>
 */
public class InnerPage implements DataPage {
    /** A logging object for reporting anything interesting that happens. */
    private static Logger logger = Logger.getLogger(InnerPage.class);


    /**
     * The offset where the number of pointer entries is stored in the page.
     * The page will hold one fewer tuples than pointers, since each tuple
     * must be sandwiched between two pointers.
     */
    public static final int OFFSET_NUM_POINTERS = 3;


    /** The offset of the first pointer in the non-leaf page. */
    public static final int OFFSET_FIRST_POINTER = 5;


    /** The actual data page that holds the B<sup>+</sup> tree inner node. */
    private DBPage dbPage;


    /** The schema of the tuples in the leaf page. */
    private Schema schema;


    /** The number of pointers stored within this non-leaf page. */
    private int numPointers;


    /**
     * An array of the offsets where the pointers are stored in this non-leaf
     * page.  Each pointer points to another page within the file.  There is
     * one more pointer than the number of tuples, since each tuple must be
     * sandwiched between two pointers.
     */
    private int[] pointerOffsets;


    /** An array of the tuples stored in this non-leaf page. */
    private BTreeFilePageTuple[] keys;


    /**
     * The total size of all data (pointers + tuples + initial values) stored
     * within this non-leaf page.  This is also the offset at which we can
     * start writing more data without overwriting anything.
     */
    private int endOffset;


    /**
     * Initialize the inner-page wrapper class for the specified B<sup>+</sup>
     * tree leaf page.  The contents of the inner-page are cached in the
     * fields of the wrapper object.
     *
     * @param dbPage the data page from the B<sup>+</sup> Tree file to wrap
     * @param schema the schema of tuples stored in the data page
     */
    public InnerPage(DBPage dbPage, Schema schema) {
        if (dbPage.readUnsignedByte(0) != BTREE_INNER_PAGE) {
            throw new IllegalArgumentException("Specified DBPage " +
                dbPage.getPageNo() + " is not marked as an inner page.");
        }

        this.dbPage = dbPage;
        this.schema = schema;

        loadPageContents();
    }


    /**
     * This static helper function initializes a {@link DBPage} object's
     * contents with the type and detail values that will allow a new
     * {@code InnerPage} wrapper to be instantiated for the page, and then it
     * returns a wrapper object for the page.  This version of the {@code init}
     * function creates an inner page that is initially empty.
     *
     * @param dbPage the page to initialize as an inner page.
     *
     * @param schema the schema of the tuples in the leaf page
     *
     * @return a newly initialized {@code InnerPage} object wrapping the page
     */
    public static InnerPage init(DBPage dbPage, Schema schema) {
        dbPage.writeByte(OFFSET_PAGE_TYPE, BTREE_INNER_PAGE);
        dbPage.writeShort(OFFSET_NUM_POINTERS, 0);

        return new InnerPage(dbPage, schema);
    }


    /**
     * This static helper function initializes a {@link DBPage} object's
     * contents with the type and detail values that will allow a new
     * {@code InnerPage} wrapper to be instantiated for the page, and then it
     * returns a wrapper object for the page.  This version of the {@code init}
     * function creates an inner page that initially contains the specified
     * page-pointers and key value.
     *
     * @param dbPage the page to initialize as an inner page.
     *
     * @param schema the schema of the tuples in the inner page
     *
     * @param pagePtr1 the first page-pointer to store in the inner page, to the
     *        left of {@code key1}
     *
     * @param key1 the first key to store in the inner page
     *
     * @param pagePtr2 the second page-pointer to store in the inner page, to
     *        the right of {@code key1}
     *
     * @return a newly initialized {@code InnerPage} object wrapping the page
     */
    public static InnerPage init(DBPage dbPage, Schema schema,
                                 int pagePtr1, Tuple key1, int pagePtr2) {

        dbPage.writeByte(OFFSET_PAGE_TYPE, BTREE_INNER_PAGE);

        // Write the first contents of the non-leaf page:  [ptr0, key0, ptr1]
        // Since key0 will usually be a BTreeFilePageTuple, we have to rely on
        // the storeTuple() method to tell us where the new tuple's data ends.

        int offset = OFFSET_FIRST_POINTER;

        dbPage.writeShort(offset, pagePtr1);
        offset += 2;

        offset = PageTuple.storeTuple(dbPage, offset, schema, key1);

        dbPage.writeShort(offset, pagePtr2);

        dbPage.writeShort(OFFSET_NUM_POINTERS, 2);

        return new InnerPage(dbPage, schema);
    }


    /**
     * This private helper scans through the inner page's contents and caches
     * the contents of the inner page in a way that makes it easy to use and
     * manipulate.
     */
    private void loadPageContents() {
        numPointers = dbPage.readUnsignedShort(OFFSET_NUM_POINTERS);
        if (numPointers > 0) {
            pointerOffsets = new int[numPointers];
            keys = new BTreeFilePageTuple[numPointers - 1];

            // Handle first pointer + key separately since we know their offsets

            pointerOffsets[0] = OFFSET_FIRST_POINTER;

            if (numPointers == 1) {
                // This will happen when we are deleting values from a page.
                // No keys, just 1 pointer, done!
                endOffset = OFFSET_FIRST_POINTER + 2;
                return;
            }

            BTreeFilePageTuple key = new BTreeFilePageTuple(schema, dbPage,
                OFFSET_FIRST_POINTER + 2, 0);
            keys[0] = key;

            // Handle all the pointer/key pairs.  This excludes the last
            // pointer.

            int keyEndOffset;
            for (int i = 1; i < numPointers - 1; i++) {
                // Next pointer starts where the previous key ends.
                keyEndOffset = key.getEndOffset();
                pointerOffsets[i] = keyEndOffset;
                
                // Next key starts after the next pointer.
                key = new BTreeFilePageTuple(schema, dbPage, keyEndOffset + 2, i);
                keys[i] = key;
            }

            keyEndOffset = key.getEndOffset();
            pointerOffsets[numPointers - 1] = keyEndOffset;
            endOffset = keyEndOffset + 2;
        }
        else {
            // There are no entries (pointers + keys).
            endOffset = OFFSET_FIRST_POINTER;
            pointerOffsets = null;
            keys = null;
        }
    }


    /**
     * Returns the {@code DBPage} that backs this leaf page.
     *
     * @return the {@code DBPage} that backs this leaf page.
     */
    public DBPage getDBPage() {
        return dbPage;
    }


    /**
     * Returns the page-number of this leaf page.
     *
     * @return the page-number of this leaf page.
     */
    public int getPageNo() {
        return dbPage.getPageNo();
    }


    /**
     * Given a leaf page in the index, returns the page number of the left
     * sibling, or -1 if there is no left sibling to this node.
     *
     * @param pagePath the page path from root to this leaf page
     * @param innerOps the inner page ops that allows this method to
     *        load inner pages and navigate the tree
     *
     * @return the page number of the left sibling leaf-node, or -1 if there
     *         is no left sibling
     *
     * @review (Donnie) There is a lot of implementation-overlap between this
     *         function and the {@link InnerPage#getLeftSibling}.  Maybe find
     *         a way to combine the implementations.
     */
    public int getLeftSibling(List<Integer> pagePath,
                              InnerPageOperations innerOps) throws IOException {

        // Verify that the last node in the page path is in fact this page.
        if (pagePath.get(pagePath.size() - 1) != getPageNo()) {
            throw new IllegalArgumentException(
                    "The page path provided does not terminate on this leaf page.");
        }

        // If this leaf doesn't have a parent, we already know it doesn't
        // have a sibling.
        if (pagePath.size() <= 1)
            return -1;

        int parentPageNo = pagePath.get(pagePath.size() - 2);
        InnerPage inner = innerOps.loadPage(parentPageNo);

        // Get the index of the pointer that points to this page.  If it
        // doesn't appear in the parent, we have a serious problem...
        int pageIndex = inner.getIndexOfPointer(getPageNo());
        if (pageIndex == -1) {
            throw new IllegalStateException(String.format(
                    "Leaf node %d doesn't appear in parent inner node %d!",
                    getPageNo(), parentPageNo));
        }

        int leftSiblingIndex = pageIndex - 1;
        int leftSiblingPageNo = -1;

        if (leftSiblingIndex >= 0)
            leftSiblingPageNo = inner.getPointer(leftSiblingIndex);

        return leftSiblingPageNo;
    }


    /**
     * Given a leaf page in the index, returns the page number of the right
     * sibling, or -1 if there is no right sibling to this node.
     *
     * @param pagePath the page path from root to this leaf page
     * @param innerOps the inner page ops that allows this method to
     *        load inner pages and navigate the tree
     *
     * @return the page number of the right sibling leaf-node, or -1 if there
     *         is no right sibling
     *
     * @review (Donnie) There is a lot of implementation-overlap between this
     *         function and the {@link InnerPage#getLeftSibling}.  Maybe find
     *         a way to combine the implementations.
     */
    public int getRightSibling(List<Integer> pagePath,
                               InnerPageOperations innerOps) throws IOException {

        // Verify that the last node in the page path is in fact this page.
        if (pagePath.get(pagePath.size() - 1) != getPageNo()) {
            throw new IllegalArgumentException(
                    "The page path provided does not terminate on this inner page.");
        }

        // If this leaf doesn't have a parent, we already know it doesn't
        // have a sibling.
        if (pagePath.size() <= 1)
            return -1;

        int parentPageNo = pagePath.get(pagePath.size() - 2);
        InnerPage inner = innerOps.loadPage(parentPageNo);

        // Get the index of the pointer that points to this page.  If it
        // doesn't appear in the parent, we have a serious problem...
        int pageIndex = inner.getIndexOfPointer(getPageNo());
        if (pageIndex == -1) {
            throw new IllegalStateException(String.format(
                    "Inner node %d doesn't appear in parent inner node %d!",
                    getPageNo(), parentPageNo));
        }

        int rightSiblingIndex = pageIndex + 1;
        int rightSiblingPageNo = -1;

        if (rightSiblingIndex < inner.getNumPointers())
            rightSiblingPageNo = inner.getPointer(rightSiblingIndex);

        return rightSiblingPageNo;
    }


    /**
     * Returns the number of pointers currently stored in this inner page.  The
     * number of keys is always one less than the number of pointers, since
     * each key must have a pointer on both sides.
     *
     * @return the number of pointers in this inner page.
     */
    public int getNumPointers() {
        return numPointers;
    }


    /**
     * Returns the number of keys currently stored in this inner page.  The
     * number of keys is always one less than the number of pointers, since
     * each key must have a pointer on both sides.
     *
     * @return the number of keys in this inner page.
     *
     * @throws IllegalStateException if the inner page contains 0 pointers
     */
    public int getNumKeys() {
        if (numPointers < 1) {
            throw new IllegalStateException("Inner page contains no " +
                "pointers.  Number of keys is meaningless.");
        }
        
        return numPointers - 1;
    }


    /**
     * Returns the total amount of space used in this page, in bytes.
     *
     * @return the total amount of space used in this page, in bytes.
     */
    public int getUsedSpace() {
        return endOffset;
    }


    /**
     * Returns the amount of space used by key/pointer entries in this page,
     * in bytes.
     *
     * @return the amount of space used by key/pointer entries in this page,
     *         in bytes.
     */
    public int getSpaceUsedByEntries() {
        return endOffset - OFFSET_FIRST_POINTER;
    }


    /**
     * Returns the amount of space available in this inner page, in bytes.
     *
     * @return the amount of space available in this inner page, in bytes.
     */
    public int getFreeSpace() {
        return dbPage.getPageSize() - endOffset;
    }


    /**
     * Returns the total space (page size) in bytes.
     *
     * @return the size of the page, in bytes.
     */
    public int getTotalSpace() {
        return dbPage.getPageSize();
    }


    /**
     * Returns the pointer at the specified index.
     *
     * @param index the index of the pointer to retrieve
     *
     * @return the pointer at that index
     */
    public int getPointer(int index) {
        return dbPage.readUnsignedShort(pointerOffsets[index]);
    }


    /**
     * Replaces one page-pointer in the inner page with another page-pointer.
     * This is used when the B<sup>+</sup> tree is being optimized, as the
     * layout of the data on disk is rearranged.
     *
     * @param index the index of the pointer to replace
     * @param newPageNo the page number to store at the specified index
     */
    public void replacePointer(int index, int newPageNo) {
        dbPage.writeShort(pointerOffsets[index], newPageNo);
        loadPageContents();
    }


    /**
     * Returns the key at the specified index.
     *
     * @param index the index of the key to retrieve
     *
     * @return the key at that index
     */
    public BTreeFilePageTuple getKey(int index) {
        return keys[index];
    }


    /**
     * This helper method scans the inner page for the specified page-pointer,
     * returning the index of the pointer if it is found, or -1 if the pointer
     * is not found.
     *
     * @param pointer the page-pointer to find in this inner page
     *
     * @return the index of the page-pointer if found, or -1 if not found
     */
    public int getIndexOfPointer(int pointer) {
        for (int i = 0; i < getNumPointers(); i++) {
            if (getPointer(i) == pointer)
                return i;
        }

        return -1;
    }


    public void replaceTuple(int index, Tuple key) {
        int oldStart = keys[index].getOffset();
        int oldLen = keys[index].getEndOffset() - oldStart;

        int newLen = PageTuple.getTupleStorageSize(schema, key);
        
        if (newLen != oldLen) {
            // Need to adjust the amount of space the key takes.
            
            if (endOffset + newLen - oldLen > dbPage.getPageSize()) {
                throw new IllegalArgumentException(
                    "New key-value is too large to fit in non-leaf page.");
            }

            dbPage.moveDataRange(oldStart + oldLen, oldStart + newLen,
                endOffset - oldStart - oldLen);
        }

        PageTuple.storeTuple(dbPage, oldStart, schema, key);

        // Reload the page contents.
        // TODO:  This is slow, but it should be fine for now.
        loadPageContents();
    }


    /**
     * This method inserts a new key and page-pointer into the inner page,
     * immediately following the page-pointer {@code pagePtr1}, which must
     * already appear within the page.  The caller is expected to have already
     * verified that the new key and page-pointer are able to fit in the page.
     *
     * @param pagePtr1 the page-pointer which should appear before the new key
     *        in the inner page.  <b>This is required to already appear within
     *        the inner page.</b>
     *
     * @param key1 the new key to add to the inner page, immediately after the
     *        {@code pagePtr1} value.
     *
     * @param pagePtr2 the new page-pointer to add to the inner page,
     *        immediately after the {@code key1} value.
     *
     * @throws IllegalArgumentException if the specified {@code pagePtr1} value
     *         cannot be found in the inner page, or if the new key and
     *         page-pointer won't fit within the space available in the page.
     */
    public void addEntry(int pagePtr1, Tuple key1, int pagePtr2) {

        if (logger.isTraceEnabled()) {
            logger.trace("Non-leaf page " + getPageNo() +
                " contents before adding entry:\n" + toFormattedString());
        }

        int i;
        for (i = 0; i < numPointers; i++) {
            if (getPointer(i) == pagePtr1)
                break;
        }
        
        logger.debug(String.format("Found page-pointer %d in index %d",
            pagePtr1, i));

        if (i == numPointers) {
            throw new IllegalArgumentException(
                "Can't find initial page-pointer " + pagePtr1 +
                " in non-leaf page " + getPageNo());
        }
        
        // Figure out where to insert the new key and value.

        int oldKeyStart;
        if (i < numPointers - 1) {
            // There's a key i associated with pointer i.  Use the key's offset,
            // since it's after the pointer.
            oldKeyStart = keys[i].getOffset();
        }
        else {
            // The pageNo1 pointer is the last pointer in the sequence.  Use
            // the end-offset of the data in the page.
            oldKeyStart = endOffset;
        }
        int len = endOffset - oldKeyStart;

        // Compute the size of the new key and pointer, and make sure they fit
        // into the page.

        int newKeySize = PageTuple.getTupleStorageSize(schema, key1);
        int newEntrySize = newKeySize + 2;
        if (endOffset + newEntrySize > dbPage.getPageSize()) {
            throw new IllegalArgumentException("New key-value and " +
                "page-pointer are too large to fit in non-leaf page.");
        }

        if (len > 0) {
            // Move the data after the pageNo1 pointer to make room for
            // the new key and pointer.
            dbPage.moveDataRange(oldKeyStart, oldKeyStart + newEntrySize, len);
        }

        // Write in the new key/pointer values.
        PageTuple.storeTuple(dbPage, oldKeyStart, schema, key1);
        dbPage.writeShort(oldKeyStart + newKeySize, pagePtr2);

        // Finally, increment the number of pointers in the page, then reload
        // the cached data.

        dbPage.writeShort(OFFSET_NUM_POINTERS, numPointers + 1);

        loadPageContents();

        if (logger.isTraceEnabled()) {
            logger.trace("Non-leaf page " + getPageNo() +
                " contents after adding entry:\n" + toFormattedString());
        }
    }


    /**
     * This function will delete a page-pointer from this inner page, along
     * with the key either to the left or to the right of the pointer.  It is
     * up to the caller to determine whether the left key or the right key
     * should be deleted.
     *
     * @param pagePtr the page-pointer value to identify and remove
     *
     * @param removeRightKey a flag specifying whether the key to the right
     *        ({@code true}) or to the left ({@code false}) should be removed
     */
    public void deletePointer(int pagePtr, boolean removeRightKey) {
        logger.debug("Trying to delete page-pointer " + pagePtr +
            " from inner page " + getPageNo() + ", and remove the " +
                (removeRightKey ? "right" : "left") + " key.");

        int ptrIndex = getIndexOfPointer(pagePtr);
        if (ptrIndex == -1) {
            throw new IllegalArgumentException(String.format(
                "Can't find page-pointer %d in inner page %d", pagePtr,
                dbPage.getPageNo()));
        }

        if (ptrIndex == 0 && !removeRightKey) {
            throw new IllegalArgumentException(String.format(
                "Tried to delete page-pointer %d and the key to the left," +
                " in inner page %d, but the pointer has no left key.",
                pagePtr, dbPage.getPageNo()));
        }

        if (ptrIndex == numPointers - 1 && removeRightKey) {
            throw new IllegalArgumentException(String.format(
                "Tried to delete page-pointer %d and the key to the right," +
                " in inner page %d, but the pointer has no right key.",
                pagePtr, dbPage.getPageNo()));
        }

        // Figure out the range of data that must be deleted from the page.

        // Page pointers are 2 bytes.
        int start = pointerOffsets[ptrIndex];
        int end = start + 2;

        // We must always remove one key, either the left or the right key.
        // Expand the data range that we are removing.
        if (removeRightKey) {
            // Remove the key to the right of the page-pointer.
            end = keys[ptrIndex].getEndOffset();

            logger.debug(String.format("Removing right key, with size %d." +
                "  Range being removed is [%d, %d).", keys[ptrIndex].getSize(),
                start, end));
        }
        else {
            // Remove the key to the left of the page-pointer.
            start = keys[ptrIndex - 1].getOffset();

            logger.debug(String.format("Removing left key, with size %d." +
                "  Range being removed is [%d, %d).", keys[ptrIndex - 1].getSize(),
                start, end));
        }

        logger.debug("Moving inner-page data in range [" + end + ", " +
                endOffset + ") over by " + (end - start) + " bytes");
        dbPage.moveDataRange(end, start, endOffset - end);

        // Decrement the total number of pointers.
        dbPage.writeShort(OFFSET_NUM_POINTERS, numPointers - 1);
        
        logger.debug("Loading altered page - had " + numPointers + 
            " pointers before delete.");
        // Load new page.
        loadPageContents();
        
        logger.debug("After loading, have " + numPointers + " pointers");
    }


    /**
     * <p>
     * This helper function moves the specified number of page-pointers to the
     * left sibling of this inner node.  The data is copied in one shot so that
     * the transfer will be fast, and the various associated bookkeeping values
     * in both inner pages are updated.
     * </p>
     * <p>
     * Of course, moving a subset of the page-pointers to a sibling will leave
     * a key without a pointer on one side; this key is promoted up to the
     * parent of the inner node.  Additionally, an existing parent-key can be
     * provided by the caller, which should be inserted before the new pointers
     * being moved into the sibling node.
     * </p>
     *
     * @param leftSibling the left sibling of this inner node in the index file
     *
     * @param count the number of pointers to move to the left sibling
     *
     * @param parentKey If this inner node and the sibling already have a parent
     *        node, this is the key between the two nodes' page-pointers in the
     *        parent node.  If the two nodes don't have a parent (i.e. because
     *        an inner node is being split into two nodes and the depth of the
     *        tree is being increased) then this value will be {@code null}.
     *
     * @return the key that should go into the parent node, between the
     *         page-pointers for this node and its sibling
     *
     * @todo (Donnie) When support for deletion is added to the index
     *       implementation, we will need to support the case when the incoming
     *       {@code parentKey} is non-{@code null}, but the returned key is
     *       {@code null} because one of the two siblings' pointers will be
     *       removed.
     */
    public TupleLiteral movePointersLeft(InnerPage leftSibling, int count,
                                         Tuple parentKey) {

        if (count < 0 || count > numPointers) {
            throw new IllegalArgumentException("count must be in range (0, " +
                numPointers + "), got " + count);
        }

        // The parent-key can be null if we are splitting a page into two pages.
        // However, this situation is only valid if the right sibling is EMPTY.
        int parentKeyLen = 0;
        if (parentKey != null) {
            parentKeyLen = PageTuple.getTupleStorageSize(schema, parentKey);
        }
        else {
            if (leftSibling.getNumPointers() != 0) {
                throw new IllegalStateException("Cannot move pointers to " +
                    "non-empty sibling if no parent-key is specified!");
            }
        }

        /* TODO:  IMPLEMENT THE REST OF THIS METHOD.
         *
         * You can use PageTuple.storeTuple() to write a key into a DBPage.
         *
         * The DBPage.write() method is useful for copying a large chunk of
         * data from one DBPage to another.
         *
         * Your implementation also needs to properly handle the incoming
         * parent-key, and produce a new parent-key as well.
         */
        logger.error("NOT YET IMPLEMENTED:  movePointersLeft()");

        // Update the cached info for both non-leaf pages.
        loadPageContents();
        leftSibling.loadPageContents();

        return null;
    }


    /**
     * Returns the page path to the right sibling, including the right sibling
     * itself. Empty list if there is none.
     * 
     * @param pagePath the page path from root to this leaf
     * @param innerOps the inner page ops that allows this method to
     *        load inner pages and navigate the tree
     *
     * @return the page path to the right sibling leaf node
     *
    public List<Integer> getRightSibling(List<Integer> pagePath,
        InnerPageOperations innerOps) {
        // Verify that the last node in the page path is this leaf page.
        if (pagePath.get(pagePath.size() - 1) != getPageNo()) {
            throw new IllegalArgumentException("The page path provided does" +
                " not terminate on this leaf page.");
        }
        
        ArrayList<Integer> rightPath = new ArrayList<Integer>();

        InnerPage inner = null;
        int index = 0;
        int i = pagePath.size() - 2;
        try {
            while (i >= 0) {
                inner = innerOps.loadPage(idxFileInfo, pagePath.get(i));
                index = inner.getIndexOfPointer(pagePath.get(i+1));
                if (index != inner.getNumPointers() - 1) {
                    // This means that the subtree this leaf is in has a right
                    // sibling subtree from the current inner node.
                    rightPath.addAll(pagePath.subList(0, i+1));
                    break;
                }
                i--;
            }

            int nextPage;
            if (inner == null || i == -1) {
                return rightPath;
            }

            // Add to the rightPath the page corresponding to one to the 
            // right of the current index
            rightPath.add(inner.getPointer(index + 1));
            i++;

            while (i <= pagePath.size() - 2) {
                index = 0;
                nextPage = inner.getPointer(index);
                rightPath.add(nextPage);
                inner = innerOps.loadPage(idxFileInfo, nextPage);
                i++;
            }
        
        } catch (IOException e) {
            throw new IllegalArgumentException("A page failed to load!");
        }

        // Can assert that for the last entry, leaf.getNextLeafPage 
        // should be last pagePath entry here
        return rightPath;
    }


    /**
     * Returns the page path to the left sibling, including the left sibling
     * itself.  Empty list if there is none.
     *
     * @param pagePath the page path from root to this leaf
     * @param innerOps the inner page ops that allows this method
     *        to load inner pages and navigate the tree
     *
     * @return the page path to the left sibling leaf node
     *
    public List<Integer> getLeftSibling(List<Integer> pagePath, InnerPageOperations innerOps) {
        // Verify that the last node in the page path is this leaf page.
        if (pagePath.get(pagePath.size() - 1) != getPageNo()) {
            throw new IllegalArgumentException("The page path provided does" +
                " not terminate on this leaf page.");
        }
        
        ArrayList<Integer> leftPath = new ArrayList<Integer>();
        // Note to self - not sure on behavior for initializing a for loop
        // with an i that does not satisfy condition.  If it doesn't do any
        // iterations, then that would be ideal behavior.  That case should
        // never occur anyways...
        InnerPage inner = null;
        int index = 0;
        int i = pagePath.size() - 2;
        try {
            while (i >= 0) {
                inner = innerOps.loadPage(idxFileInfo, pagePath.get(i));
                index = inner.getIndexOfPointer(pagePath.get(i+1));
                if (index != 0) {
                    // This means that the subtree this leaf is in has a left
                    // sibling subtree from the current inner node.
                    leftPath.addAll(pagePath.subList(0, i+1));
                    break;
                }
                i--;
            }
            
            int nextPage;
            if (inner == null || i == -1) {
                return leftPath;
            }
            // Add to the leftPath the page corresponding to one to the 
            // left of the current index
            leftPath.add(inner.getPointer(index - 1));
            i++;

            while (i <= pagePath.size() - 2) {
                index = inner.getNumPointers() - 1;
                nextPage = inner.getPointer(index);
                leftPath.add(nextPage);
                inner = innerOps.loadPage(idxFileInfo, nextPage);
                i++;
            }
        }
        catch (IOException e) {
            throw new IllegalArgumentException("A page failed to load!");
        }
        return leftPath;
    }
*/


    /**
     * <p>
     * This helper function moves the specified number of page-pointers to the
     * right sibling of this inner node.  The data is copied in one shot so that
     * the transfer will be fast, and the various associated bookkeeping values
     * in both inner pages are updated.
     * </p>
     * <p>
     * Of course, moving a subset of the page-pointers to a sibling will leave
     * a key without a pointer on one side; this key is promoted up to the
     * parent of the inner node.  Additionally, an existing parent-key can be
     * provided by the caller, which should be inserted before the new pointers
     * being moved into the sibling node.
     * </p>
     *
     * @param rightSibling the right sibling of this inner node in the index file
     *
     * @param count the number of pointers to move to the right sibling
     *
     * @param parentKey If this inner node and the sibling already have a parent
     *        node, this is the key between the two nodes' page-pointers in the
     *        parent node.  If the two nodes don't have a parent (i.e. because
     *        an inner node is being split into two nodes and the depth of the
     *        tree is being increased) then this value will be {@code null}.
     *
     * @return the key that should go into the parent node, between the
     *         page-pointers for this node and its sibling
     *
     * @todo (Donnie) When support for deletion is added to the index
     *       implementation, we will need to support the case when the incoming
     *       {@code parentKey} is non-{@code null}, but the returned key is
     *       {@code null} because one of the two siblings' pointers will be
     *       removed.
     */
    public TupleLiteral movePointersRight(InnerPage rightSibling, int count,
                                          Tuple parentKey) {

        if (count < 0 || count > numPointers) {
            throw new IllegalArgumentException("count must be in range [0, " +
                numPointers + "), got " + count);
        }

        if (logger.isTraceEnabled()) {
            logger.trace("Non-leaf page " + getPageNo() +
                " contents before moving pointers right:\n" + toFormattedString());
        }

        int startPointerIndex = numPointers - count;
        int startOffset = pointerOffsets[startPointerIndex];
        int len = endOffset - startOffset;
        
        logger.debug("Moving everything after pointer " + startPointerIndex +
            " to right sibling.  Start offset = " + startOffset +
            ", end offset = " + endOffset + ", len = " + len);

        // The parent-key can be null if we are splitting a page into two pages.
        // However, this situation is only valid if the right sibling is EMPTY.
        int parentKeyLen = 0;
        if (parentKey != null) {
            parentKeyLen = PageTuple.getTupleStorageSize(schema, parentKey);
        }
        else {
            if (rightSibling.getNumPointers() != 0) {
                throw new IllegalStateException("Cannot move pointers to " +
                    "non-empty sibling if no parent-key is specified!");
            }
        }

        /* TODO:  IMPLEMENT THE REST OF THIS METHOD.
         *
         * You can use PageTuple.storeTuple() to write a key into a DBPage.
         *
         * The DBPage.write() method is useful for copying a large chunk of
         * data from one DBPage to another.
         *
         * Your implementation also needs to properly handle the incoming
         * parent-key, and produce a new parent-key as well.
         */
        logger.error("NOT YET IMPLEMENTED:  movePointersRight()");

        // Update the cached info for both non-leaf pages.
        loadPageContents();
        rightSibling.loadPageContents();

        if (logger.isTraceEnabled()) {
            logger.trace("Non-leaf page " + getPageNo() +
                " contents after moving pointers right:\n" + toFormattedString());

            logger.trace("Right-sibling page " + rightSibling.getPageNo() +
                " contents after moving pointers right:\n" +
                rightSibling.toFormattedString());
        }

        return null;
    }


    /**
     * <p>
     * This helper method creates a formatted string containing the contents of
     * the inner page, including the pointers and the intervening keys.
     * </p>
     * <p>
     * It is strongly suggested that this method should only be used for
     * trace-level output, since otherwise the output will become overwhelming.
     * </p>
     *
     * @return a formatted string containing the contents of the inner page
     */
    public String toFormattedString() {
        StringBuilder buf = new StringBuilder();

        buf.append(String.format("Inner page %d contains %d pointers%n",
            getPageNo(), numPointers));

        if (numPointers > 0) {
            for (int i = 0; i < numPointers - 1; i++) {
                buf.append(String.format("    Pointer %d = page %d%n", i,
                    getPointer(i)));
                buf.append(String.format("    Key %d = %s%n", i, getKey(i)));
            }
            buf.append(String.format("    Pointer %d = page %d%n", numPointers - 1,
                getPointer(numPointers - 1)));
        }

        return buf.toString();
    }
}
