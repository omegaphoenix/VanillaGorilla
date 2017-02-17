package edu.caltech.nanodb.storage.btreefile;


import java.io.IOException;
import java.util.List;

import org.apache.log4j.Logger;

import edu.caltech.nanodb.expressions.TupleLiteral;
import edu.caltech.nanodb.relations.Tuple;
import edu.caltech.nanodb.storage.DBFile;
import edu.caltech.nanodb.storage.DBPage;
import edu.caltech.nanodb.storage.PageTuple;
import edu.caltech.nanodb.storage.StorageManager;


/**
 * This class provides high-level B<sup>+</sup> tree management operations
 * performed on inner nodes.  These operations are provided here and not on the
 * {@link InnerPage} class since they sometimes involve splitting or merging
 * inner nodes, updating parent nodes, and so forth.
 */
public class InnerPageOperations {

    /**
     * When deleting a page-pointer from an inner page, either the left or
     * right key must also be removed; this constant specifies removal of the
     * left key.
     *
     * @see #deletePointer
     */
    public static final int REMOVE_KEY_TO_LEFT = 0;


    /**
     * When deleting a page-pointer from an inner page, either the left or
     * right key must also be removed; this constant specifies removal of the
     * right key.
     *
     * @see #deletePointer
     */
    public static final int REMOVE_KEY_TO_RIGHT = 1;


    /** A logging object for reporting anything interesting that happens. */
    private static Logger logger = Logger.getLogger(InnerPageOperations.class);


    private StorageManager storageManager;


    private BTreeTupleFile tupleFile;


    private FileOperations fileOps;


    public InnerPageOperations(StorageManager storageManager,
                               BTreeTupleFile tupleFile,
                               FileOperations fileOps) {
        this.storageManager = storageManager;
        this.tupleFile = tupleFile;
        this.fileOps = fileOps;
    }


    public InnerPage loadPage(int pageNo) throws IOException {
        DBFile dbFile = tupleFile.getDBFile();
        DBPage dbPage = storageManager.loadDBPage(dbFile, pageNo);
        return new InnerPage(dbPage, tupleFile.getSchema());
    }


    /**
     * This helper function is used to update a key between two existing
     * pointers in an inner B<sup>+</sup> tree node.  It is an error if the
     * specified pair of pointers cannot be found in the node.
     *
     * @param page the inner page to update the key in
     * @param pagePath the path to the page, from the root node
     * @param pagePtr1 the pointer P<sub>i</sub> before the key to update
     * @param key1 the new value of the key K<sub>i</sub> to store
     * @param pagePtr2 the pointer P<sub>i+1</sub> after the key to update
     *
     * @todo (Donnie) This implementation has a major failing that will occur
     *       infrequently - if the inner page doesn't have room for the new key
     *       (e.g. if the page was already almost full, and then the new key is
     *       larger than the old key) then the inner page needs to be split,
     *       per usual.  Right now it will just throw an exception in this case.
     *       This is why the {@code pagePath} argument is provided, so that when
     *       this bug is fixed, the page-path will be available.
     */
    public void replaceTuple(InnerPage page, List<Integer> pagePath,
        int pagePtr1, Tuple key1, int pagePtr2) throws IOException {

        for (int i = 0; i < page.getNumPointers() - 1; i++) {
            if (page.getPointer(i) == pagePtr1 &&
                page.getPointer(i + 1) == pagePtr2) {

                // Found the pair of pointers!  Replace the key-value.

                BTreeFilePageTuple oldKey = page.getKey(i);
                int oldKeySize = oldKey.getSize();

                int newKeySize =
                    PageTuple.getTupleStorageSize(tupleFile.getSchema(), key1);

                if (logger.isDebugEnabled()) {
                    logger.debug(String.format("Inner page %d:  replacing " +
                        "old key of %d bytes (between pointers %d and %d) " +
                        "with new key of %d bytes", page.getPageNo(),
                        oldKeySize, pagePtr1, pagePtr2, newKeySize));
                }

                if (logger.isTraceEnabled()) {
                    logger.trace("Contents of inner page " + page.getPageNo() +
                        " before replacement:\n" + page.toFormattedString());
                }

                if (page.getFreeSpace() + oldKeySize - newKeySize >= 0) {
                    // We have room - go ahead and do this.
                    page.replaceTuple(i, key1);

                    // Make sure we didn't cause any brain damage...
                    assert page.getPointer(i) == pagePtr1;
                    assert page.getPointer(i + 1) == pagePtr2;
                }
                else {
                    // We need to make more room in this inner page, either by
                    // relocating records or by splitting this page.  We will
                    // do this by deleting the old entry, and then adding the
                    // new entry, so that we can leverage all our good code
                    // for splitting/relocating/etc.

                    logger.info(String.format("Not enough space in page %d;" +
                        " trying to relocate entries / split page.",
                        page.getPageNo()));

                    // Delete pagePtr2, and the key to the LEFT of it.
                    page.deletePointer(pagePtr2, /* removeRightKey */ false);

                    // Now add new key, and put pagePtr2 back after pagePtr1.
                    addTuple(page, pagePath, pagePtr1, key1, pagePtr2);
                }

                if (logger.isTraceEnabled()) {
                    logger.trace("Contents of inner page " + page.getPageNo() +
                        " after replacement:\n" + page.toFormattedString());
                }

                return;
            }
        }

        // If we got here, we have a big problem.  Couldn't find the expected
        // pair of pointers we were handed.

        // Dump the page contents because presumably we want to figure out
        // what is going on...
        logger.error(String.format(
            "Couldn't find pair of pointers %d and %d in inner page %d!",
            pagePtr1, pagePtr2, page.getPageNo()));
        logger.error("Page contents:\n" + page.toFormattedString());

        throw new IllegalStateException(
            "Couldn't find sequence of page-pointers [" + pagePtr1 + ", " +
                pagePtr2 + "] in non-leaf page " + page.getPageNo());
    }


    /**
     * This helper function determines how many pointers must be relocated from
     * one inner page to another, in order to free up the specified number of
     * bytes.  If it is possible, the number of pointers that must be relocated
     * is returned.  If it is not possible, the method returns 0.
     *
     * @param page the inner page to relocate entries from
     *
     * @param adjPage the adjacent page (predecessor or successor) to relocate
     *        entries to
     *
     * @param movingRight pass {@code true} if the sibling is to the right of
     *        {@code page} (and therefore we are moving entries right), or
     *        {@code false} if the sibling is to the left of {@code page} (and
     *        therefore we are moving entries left).
     *
     * @param bytesRequired the number of bytes that must be freed up in
     *        {@code page} by the operation
     *
     * @param parentKeySize the size of the parent key that must also be
     *        relocated into the adjacent page, and therefore affects how many
     *        pointers can be transferred
     *
     * @return the number of pointers that must be relocated to free up the
     *         required space, or 0 if it is not possible.
     */
    private int tryNonLeafRelocateForSpace(InnerPage page, InnerPage adjPage,
        boolean movingRight, int bytesRequired, int parentKeySize) {

        int numKeys = page.getNumKeys();
        int pageBytesFree = page.getFreeSpace();
        int adjBytesFree = adjPage.getFreeSpace();

        logger.debug(String.format("Trying to relocate records from inner-" +
            "page %d (%d bytes free) to adjacent inner-page %d (%d bytes " +
            "free), moving %s, to free up %d bytes.  Parent key size is %d " +
            "bytes.", page.getPageNo(), pageBytesFree, adjPage.getPageNo(),
            adjBytesFree, (movingRight ? "right" : "left"), bytesRequired,
            parentKeySize));

        // The parent key always has to move to the adjacent page, so if that
        // won't fit, don't even try.
        if (adjBytesFree < parentKeySize) {
            logger.debug(String.format("Adjacent page %d has %d bytes free;" +
                " not enough to hold %d-byte parent key.  Giving up on " +
                "relocation.", adjPage.getPageNo(), adjBytesFree, parentKeySize));
            return 0;
        }

        // Since the parent key must always be rotated down into the adjacent
        // inner page, we must account for that space.
        adjBytesFree -= parentKeySize;

        int keyBytesMoved = 0;

        int numRelocated = 0;
        while (true) {
            // Figure out the index of the key we need the size of, based on
            // the direction we are moving entries, and how many we have
            // already moved.  If we are moving entries right, we must start
            // with the rightmost entry in page.  If we are moving entries
            // left, we must start with the leftmost entry in page.
            int index;
            if (movingRight)
                index = numKeys - numRelocated - 1;
            else
                index = numRelocated;

            // Add two bytes to the key size for the page-pointer that follows
            int entrySize = page.getKey(index).getSize() + 2;
            logger.debug("Entry " + index + " is " + entrySize + " bytes");

            // Did we run out of space to move entries before we hit our goal?
            if (adjBytesFree < entrySize) {
                numRelocated = 0;
                break;
            }

            numRelocated++;

            pageBytesFree += entrySize;
            adjBytesFree -= entrySize;

            // Since we don't yet know which page the new pointer will go into,
            // stop when we can put the pointer in either page.
            if (pageBytesFree >= bytesRequired &&
                adjBytesFree >= bytesRequired) {
                break;
            }
        }

        assert numRelocated >= 0;
        return numRelocated;
    }


    /**
     * This helper function adds an entry (a key and associated pointer) to
     * this inner page, after the page-pointer {@code pagePtr1}.
     *
     * @param page the inner page to add the entry to
     *
     * @param pagePath the path of page-numbers to this inner page
     *
     * @param pagePtr1 the <u>existing</u> page that the new key and next-page
     *        number will be inserted after
     *
     * @param key1 the new key-value to insert after the {@code pagePtr1} value
     *
     * @param pagePtr2 the new page-pointer value to follow the {@code key1}
     *        value
     *
     * @throws IOException if an IO error occurs while updating the index
     */
    public void addTuple(InnerPage page, List<Integer> pagePath,
        int pagePtr1, Tuple key1, int pagePtr2) throws IOException {

        // The new entry will be the key, plus 2 bytes for the page-pointer.
        int newEntrySize =
            PageTuple.getTupleStorageSize(tupleFile.getSchema(), key1) + 2;

        logger.debug(String.format("Adding new %d-byte entry to inner page %d",
            newEntrySize, page.getPageNo()));

        if (page.getFreeSpace() < newEntrySize) {
            logger.debug("Not enough room in inner page " + page.getPageNo() +
                "; trying to relocate entries to make room");

            // Try to relocate entries from this inner page to either sibling,
            // or if that can't happen, split the inner page into two.
            if (!relocatePointersAndAddKey(page, pagePath,
                pagePtr1, key1, pagePtr2, newEntrySize)) {
                logger.debug("Couldn't relocate enough entries to make room;" +
                    " splitting page " + page.getPageNo() + " instead");
                splitAndAddKey(page, pagePath, pagePtr1, key1, pagePtr2);
            }
        }
        else {
            // There is room in the leaf for the new key.  Add it there.
            page.addEntry(pagePtr1, key1, pagePtr2);
        }

    }


    /**
     * This function will delete the specified Key/Pointer pair from the
     * passed-in inner page.
     *
     * @param page the inner page to delete the key/pointer from
     *
     * @param pagePath the page path to the passed page
     *
     * @param pagePtr the page-pointer value to identify and remove
     *
     * @param removeRightKey a flag specifying whether the key to the right
     *        ({@code true}) or to the left ({@code false}) should be removed
     */
    public void deletePointer(InnerPage page, List<Integer> pagePath,
        int pagePtr, boolean removeRightKey) throws IOException {

        page.deletePointer(pagePtr, removeRightKey);

        if (page.getUsedSpace() >= page.getTotalSpace() / 2) {
            // The page is at least half-full.  Don't need to redistribute or
            // coalesce.
            return;
        }
        else if (pagePath.size() == 1) {
            // The page is the root.  Don't need to redistribute or coalesce,
            // but if the root is now empty, need to shorten the tree depth.

            if (page.getNumKeys() == 0) {
                logger.debug(String.format("Current root page %d is now " +
                    "empty, removing.", page.getPageNo()));

                // Set the index's new root page.
                DBPage dbpHeader =
                    storageManager.loadDBPage(tupleFile.getDBFile(), 0);
                HeaderPage.setRootPageNo(dbpHeader, page.getPointer(0));

                // Free up this page in the index.
                fileOps.releaseDataPage(page.getDBPage());
            }
            return;
        }

        // If we got to this part, we have to redistribute/coalesce stuff :(

        // Note: Assumed that at least one of the siblings has the same
        // immediate parent.  If that is not the case... we're doomed...
        // TODO:  VERIFY THIS AND THROW AN EXCEPTION IF NOT

        int pageNo = page.getPageNo();

        int leftPageNo = page.getLeftSibling(pagePath, this);
        int rightPageNo = page.getRightSibling(pagePath, this);

        if (leftPageNo == -1 && rightPageNo == -1) {
            // We should never get to this point, since the earlier test
            // should have caught this situation.
            throw new IllegalStateException(String.format(
                "Inner node %d doesn't have a left or right sibling!",
                page.getPageNo()));
        }

        // Now we know that at least one sibling is present.  Load both
        // siblings and coalesce/redistribute in the direction that makes
        // the most sense...

        InnerPage leftSibling = null;
        if (leftPageNo != -1)
            leftSibling = loadPage(leftPageNo);

        InnerPage rightSibling = null;
        if (rightPageNo != -1)
            rightSibling = loadPage(rightPageNo);

        // Relocating or coalescing entries requires updating the parent node.
        // Since the current node has a sibling, it must also have a parent.

        int parentPageNo = pagePath.get(pagePath.size() - 2);

        InnerPage parentPage = loadPage(parentPageNo);
        // int numPointers = parentPage.getNumPointers();
        int indexInParentPage = parentPage.getIndexOfPointer(page.getPageNo());

        // See if we can coalesce the node into its left or right sibling.
        // When we do the check, we must not forget that each node contains a
        // header, and we need to account for that space as well.  This header
        // space is included in the getUsedSpace() method, but is excluded by
        // the getSpaceUsedByTuples() method.

        // TODO:  SEE IF WE CAN SIMPLIFY THIS AT ALL...
        if (leftSibling != null &&
            leftSibling.getUsedSpace() + page.getSpaceUsedByEntries() <
            leftSibling.getTotalSpace()) {

            // Coalesce the current node into the left sibling.
            logger.debug("Delete from inner page " + pageNo +
                ":  coalescing with left sibling page.");

            logger.debug(String.format("Before coalesce-left, page has %d " +
                "pointers and left sibling has %d pointers.",
                page.getNumPointers(), leftSibling.getNumPointers()));

            // The affected key in the parent page is to the left of this
            // page's index in the parent page, since we are moving entries
            // to the left sibling.
            Tuple parentKey = parentPage.getKey(indexInParentPage - 1);

            // We don't care about the key returned by movePointersLeft(),
            // since we are deleting the parent key anyway.
            page.movePointersLeft(leftSibling, page.getNumPointers(), parentKey);

            logger.debug(String.format("After coalesce-left, page has %d " +
                "pointers and left sibling has %d pointers.",
                page.getNumPointers(), leftSibling.getNumPointers()));

            // Free up the page since it's empty now
            fileOps.releaseDataPage(page.getDBPage());
            page = null;

            List<Integer> parentPagePath = pagePath.subList(0, pagePath.size() - 1);
            deletePointer(parentPage, parentPagePath, pageNo,
                /* delete right key */ false);
        }
        else if (rightSibling != null &&
                rightSibling.getUsedSpace() + page.getSpaceUsedByEntries() <
                        rightSibling.getTotalSpace()) {

            // Coalesce the current node into the right sibling.
            logger.debug("Delete from leaf " + pageNo +
                    ":  coalescing with right sibling leaf.");

            logger.debug(String.format("Before coalesce-right, page has %d " +
                    "keys and right sibling has %d pointers.",
                    page.getNumPointers(), rightSibling.getNumPointers()));

            // The affected key in the parent page is to the right of this
            // page's index in the parent page, since we are moving entries
            // to the right sibling.
            Tuple parentKey = parentPage.getKey(indexInParentPage);

            // We don't care about the key returned by movePointersRight(),
            // since we are deleting the parent key anyway.
            page.movePointersRight(rightSibling, page.getNumPointers(), parentKey);

            logger.debug(String.format("After coalesce-right, page has %d " +
                    "entries and right sibling has %d pointers.",
                    page.getNumPointers(), rightSibling.getNumPointers()));

            // Free up the right page since it's empty now
            fileOps.releaseDataPage(page.getDBPage());
            page = null;

            List<Integer> parentPagePath = pagePath.subList(0, pagePath.size() - 1);
            deletePointer(parentPage, parentPagePath, pageNo,
                /* delete right key */ true);
        }
        else {
            // Can't coalesce the leaf node into either sibling.  Redistribute
            // entries from left or right sibling into the leaf.  The strategy
            // is as follows:

            // If the node has both left and right siblings, redistribute from
            // the fuller sibling.  Otherwise, just redistribute from
            // whichever sibling we have.

            InnerPage adjPage = null;
            if (leftSibling != null && rightSibling != null) {
                // Both siblings are present.  Choose the fuller one to
                // relocate from.
                if (leftSibling.getUsedSpace() > rightSibling.getUsedSpace())
                    adjPage = leftSibling;
                else
                    adjPage = rightSibling;
            }
            else if (leftSibling != null) {
                // There is no right sibling.  Use the left sibling.
                adjPage = leftSibling;
            }
            else {
                // There is no left sibling.  Use the right sibling.
                adjPage = rightSibling;
            }

            PageTuple parentKey;

            if (adjPage == leftSibling)
                parentKey = parentPage.getKey(indexInParentPage - 1);
            else  // adjPage == right sibling
                parentKey = parentPage.getKey(indexInParentPage);

            int entriesToMove = tryNonLeafRelocateToFill(page, adjPage,
                /* movingRight */ adjPage == leftSibling, parentKey.getSize());

            if (entriesToMove == 0) {
                // We really tried to satisfy the "minimum size" requirement,
                // but we just couldn't.  Log it and return.

                StringBuilder buf = new StringBuilder();

                buf.append(String.format("Couldn't relocate pointers to" +
                    " satisfy minimum space requirement in leaf-page %d" +
                    " with %d entries!\n", pageNo, page.getNumPointers()));

                if (leftSibling != null) {
                    buf.append(String.format("\t- Left sibling page %d has " +
                        "%d pointers\n", leftSibling.getPageNo(),
                        leftSibling.getNumPointers()));
                }
                else {
                    buf.append("\t- No left sibling\n");
                }

                if (rightSibling != null) {
                    buf.append(String.format("\t- Right sibling page %d has " +
                        "%d pointers", rightSibling.getPageNo(),
                        rightSibling.getNumPointers()));
                }
                else {
                    buf.append("\t- No right sibling");
                }

                logger.warn(buf);

                return;
            }

            logger.debug(String.format("Relocating %d pointers into page " +
                    "%d from %s sibling page %d", entriesToMove, pageNo,
                    (adjPage == leftSibling ? "left" : "right"), adjPage.getPageNo()));

            if (adjPage == leftSibling) {
                adjPage.movePointersRight(page, entriesToMove, parentKey);
                parentPage.replaceTuple(indexInParentPage - 1, page.getKey(0));
            }
            else { // adjPage == right sibling
                adjPage.movePointersLeft(page, entriesToMove, parentKey);
                parentPage.replaceTuple(indexInParentPage, adjPage.getKey(0));
            }
        }
    }


    private boolean relocatePointersAndAddKey(InnerPage page,
        List<Integer> pagePath, int pagePtr1, Tuple key1, int pagePtr2,
        int newEntrySize) throws IOException {

        int pathSize = pagePath.size();
        if (pagePath.get(pathSize - 1) != page.getPageNo()) {
            throw new IllegalArgumentException(
                "Inner page number doesn't match last page-number in page path");
        }

        // See if we are able to relocate records either direction to free up
        // space for the new key.

        if (pathSize == 1)  // This node is also the root - no parent.
            return false;   // There aren't any siblings to relocate to.

        int parentPageNo = pagePath.get(pathSize - 2);
        InnerPage parentPage = loadPage(parentPageNo);

        logger.debug(String.format("Parent of inner-page %d is page %d.",
            page.getPageNo(), parentPageNo));

        if (logger.isTraceEnabled()) {
            logger.trace("Parent page contents:\n" +
                parentPage.toFormattedString());
        }

        int numPointers = parentPage.getNumPointers();
        int pagePtrIndex = parentPage.getIndexOfPointer(page.getPageNo());

        // Check each sibling in its own code block so that we can constrain
        // the scopes of the variables a bit.  This keeps us from accidentally
        // reusing the "prev" variables in the "next" section.

        {
            InnerPage prevPage = null;
            if (pagePtrIndex - 1 >= 0)
                prevPage = loadPage(parentPage.getPointer(pagePtrIndex - 1));

            if (prevPage != null) {
                // See if we can move some of this inner node's entries to the
                // previous node, to free up space.

                BTreeFilePageTuple parentKey = parentPage.getKey(pagePtrIndex - 1);
                int parentKeySize = parentKey.getSize();

                int count = tryNonLeafRelocateForSpace(page, prevPage, false,
                    newEntrySize, parentKeySize);

                if (count > 0) {
                    // Yes, we can do it!

                    logger.debug(String.format("Relocating %d entries from " +
                        "inner-page %d to left-sibling inner-page %d", count,
                        page.getPageNo(), prevPage.getPageNo()));

                    logger.debug("Space before relocation:  Inner = " +
                        page.getFreeSpace() + " bytes\t\tSibling = " +
                        prevPage.getFreeSpace() + " bytes");

                    TupleLiteral newParentKey =
                        page.movePointersLeft(prevPage, count, parentKey);

                    // Even with relocating entries, this could fail.
                    if (!addEntryToInnerPair(prevPage, page, pagePtr1, key1, pagePtr2))
                        return false;

                    logger.debug("New parent-key is " + newParentKey);

                    pagePath.remove(pathSize - 1);
                    replaceTuple(parentPage, pagePath, prevPage.getPageNo(),
                                    newParentKey, page.getPageNo());

                    logger.debug("Space after relocation:  Inner = " +
                        page.getFreeSpace() + " bytes\t\tSibling = " +
                        prevPage.getFreeSpace() + " bytes");

                    return true;
                }
            }
        }

        {
            InnerPage nextPage = null;
            if (pagePtrIndex + 1 < numPointers)
                nextPage = loadPage(parentPage.getPointer(pagePtrIndex + 1));

            if (nextPage != null) {
                // See if we can move some of this inner node's entries to the
                // previous node, to free up space.

                BTreeFilePageTuple parentKey = parentPage.getKey(pagePtrIndex);
                int parentKeySize = parentKey.getSize();

                int count = tryNonLeafRelocateForSpace(page, nextPage, true,
                    newEntrySize, parentKeySize);

                if (count > 0) {
                    // Yes, we can do it!

                    logger.debug(String.format("Relocating %d entries from " +
                        "inner-page %d to right-sibling inner-page %d", count,
                        page.getPageNo(), nextPage.getPageNo()));

                    logger.debug("Space before relocation:  Inner = " +
                        page.getFreeSpace() + " bytes\t\tSibling = " +
                        nextPage.getFreeSpace() + " bytes");

                    TupleLiteral newParentKey =
                        page.movePointersRight(nextPage, count, parentKey);

                    // Even with relocating entries, this could fail.
                    if (!addEntryToInnerPair(page, nextPage, pagePtr1, key1, pagePtr2))
                        return false;

                    logger.debug("New parent-key is " + newParentKey);

                    pagePath.remove(pathSize - 1);
                    replaceTuple(parentPage, pagePath, page.getPageNo(),
                                    newParentKey, nextPage.getPageNo());

                    logger.debug("Space after relocation:  Inner = " +
                        page.getFreeSpace() + " bytes\t\tSibling = " +
                        nextPage.getFreeSpace() + " bytes");

                    return true;
                }
            }
        }

        // Couldn't relocate entries to either the previous or next page.  We
        // must split the leaf into two.
        return false;
    }


    /**
     * <p>
     * This helper function splits the specified inner page into two pages,
     * also updating the parent page in the process, and then inserts the
     * specified key and page-pointer into the appropriate inner page.  This
     * method is used to add a key/pointer to an inner page that doesn't have
     * enough space, when it isn't possible to relocate pointers to the left
     * or right sibling of the page.
     * </p>
     * <p>
     * When the inner node is split, half of the pointers are put into the new
     * sibling, regardless of the size of the keys involved.  In other words,
     * this method doesn't try to keep the pages half-full based on bytes used.
     * </p>
     *
     * @param page the inner node to split and then add the key/pointer to
     *
     * @param pagePath the sequence of page-numbers traversed to reach this
     *        inner node.
     *
     * @param pagePtr1 the existing page-pointer after which the new key and
     *        pointer should be inserted
     *
     * @param key1 the new key to insert into the inner page, immediately after
     *        the page-pointer value {@code pagePtr1}.
     *
     * @param pagePtr2 the new page-pointer value to insert after the new key
     *        value
     *
     * @throws IOException if an IO error occurs during the operation.
     */
    private void splitAndAddKey(InnerPage page, List<Integer> pagePath,
        int pagePtr1, Tuple key1, int pagePtr2) throws IOException {

        int pathSize = pagePath.size();
        if (pagePath.get(pathSize - 1) != page.getPageNo()) {
            throw new IllegalArgumentException(
                "Inner page number doesn't match last page-number in page path");
        }

        if (logger.isTraceEnabled()) {
            logger.trace("Initial contents of inner page " + page.getPageNo() +
                ":\n" + page.toFormattedString());
        }

        logger.debug("Splitting inner-page " + page.getPageNo() +
            " into two inner pages.");

        // Get a new blank page in the index, with the same parent as the
        // inner-page we were handed.

        DBPage newDBPage = fileOps.getNewDataPage();
        InnerPage newPage = InnerPage.init(newDBPage, tupleFile.getSchema());

        // Figure out how many values we want to move from the old page to the
        // new page.

        int numPointers = page.getNumPointers();

        if (logger.isDebugEnabled()) {
            logger.debug(String.format("Relocating %d pointers from left-page %d" +
                " to right-page %d", numPointers, page.getPageNo(), newPage.getPageNo()));
            logger.debug("    Old left # of pointers:  " + page.getNumPointers());
            logger.debug("    Old right # of pointers:  " + newPage.getNumPointers());
        }

        Tuple parentKey = null;
        InnerPage parentPage = null;

        int parentPageNo = 0;
        if (pathSize > 1)
            parentPageNo = pagePath.get(pathSize - 2);

        if (parentPageNo != 0) {
            parentPage = loadPage(parentPageNo);
            int parentPtrIndex = parentPage.getIndexOfPointer(page.getPageNo());
            if (parentPtrIndex < parentPage.getNumPointers() - 1)
                parentKey = parentPage.getKey(parentPtrIndex);
        }
        Tuple newParentKey =
            page.movePointersRight(newPage, numPointers / 2, parentKey);

        if (logger.isDebugEnabled()) {
            logger.debug("    New parent key:  " + newParentKey);
            logger.debug("    New left # of pointers:  " + page.getNumPointers());
            logger.debug("    New right # of pointers:  " + newPage.getNumPointers());
        }

        if (logger.isTraceEnabled()) {
            logger.trace("Final contents of inner page " + page.getPageNo() +
                ":\n" + page.toFormattedString());

            logger.trace("Final contents of new inner page " +
                newPage.getPageNo() + ":\n" + newPage.toFormattedString());
        }

        if (!addEntryToInnerPair(page, newPage, pagePtr1, key1, pagePtr2)) {
            // This is unexpected, but we had better report it if it happens.
            throw new IllegalStateException("UNEXPECTED:  Couldn't add " +
                "entry to half-full inner page!");
        }

        // If the current node doesn't have a parent, it's because it's
        // currently the root.
        if (parentPageNo == 0) {
            // Create a new root node and set both leaves to have it as their
            // parent.
            DBPage dbpParent = fileOps.getNewDataPage();
            parentPage = InnerPage.init(dbpParent, tupleFile.getSchema(),
                page.getPageNo(), newParentKey, newPage.getPageNo());

            parentPageNo = parentPage.getPageNo();

            // We have a new root-page in the index!
            DBFile dbFile = tupleFile.getDBFile();
            DBPage dbpHeader = storageManager.loadDBPage(dbFile, 0);
            HeaderPage.setRootPageNo(dbpHeader, parentPageNo);

            logger.debug("Set index root-page to inner-page " + parentPageNo);
        }
        else {
            // Add the new page into the parent non-leaf node.  (This may cause
            // the parent node's contents to be moved or split, if the parent
            // is full.)

            // (We already set the new node's parent-page-number earlier.)

            pagePath.remove(pathSize - 1);
            addTuple(parentPage, pagePath, page.getPageNo(), newParentKey,
                        newPage.getPageNo());

            logger.debug("Parent page " + parentPageNo + " now has " +
                parentPage.getNumPointers() + " page-pointers.");
        }

        if (logger.isTraceEnabled()) {
            logger.trace("Parent page contents:\n" +
                parentPage.toFormattedString());
        }
    }


    /**
     * This helper method takes a pair of inner nodes that are siblings to each
     * other, and adds the specified key to whichever node the key should go
     * into.
     *
     * @param prevPage the first page in the pair, left sibling of
     *        {@code nextPage}
     *
     * @param nextPage the second page in the pair, right sibling of
     *        {@code prevPage}
     *
     * @param pageNo1 the pointer to the left of the new key/pointer values that
     *        will be added to one of the pages
     *
     * @param key1 the new key-value to insert immediately after the existing
     *        {@code pageNo1} value
     *
     * @param pageNo2 the new pointer-value to insert immediately after the new
     *        {@code key1} value
     *
     * @return true if the entry was able to be added to one of the pages, or
     *         false if the entry couldn't be added.
     */
    private boolean addEntryToInnerPair(InnerPage prevPage, InnerPage nextPage,
                                        int pageNo1, Tuple key1, int pageNo2) {
        InnerPage page;

        // See if pageNo1 appears in the left page.
        int ptrIndex1 = prevPage.getIndexOfPointer(pageNo1);
        if (ptrIndex1 != -1) {
            page = prevPage;
        }
        else {
            // The pointer *should be* in the next page.  Verify this...
            page = nextPage;

            if (nextPage.getIndexOfPointer(pageNo1) == -1) {
                throw new IllegalStateException(String.format(
                    "Somehow lost page-pointer %d from inner pages %d and %d",
                    pageNo1, prevPage.getPageNo(), nextPage.getPageNo()));
            }
        }

        int entrySize = 2 +
            PageTuple.getTupleStorageSize(tupleFile.getSchema(), key1);

        if (page.getFreeSpace() >= entrySize) {
            page.addEntry(pageNo1, key1, pageNo2);
            return true;
        }
        else {
            return false;
        }
    }


    /**
     * This helper function determines how many entries must be relocated from
     * one leaf-page to another, in order to satisfy the "minimum space"
     * requirement of the B tree.free up the specified number of
     * bytes.  If it is possible, the number of entries that must be relocated
     * is returned.  If it is not possible, the method returns 0.
     *
     * @param page the inner node to relocate entries from
     *
     * @param adjPage the adjacent inner page (predecessor or successor) to
     *        relocate entries to
     *
     * @param movingRight pass {@code true} if the sibling is to the left of
     *        {@code page} (and therefore we are moving entries right), or
     *        {@code false} if the sibling is to the right of {@code page}
     *        (and therefore we are moving entries left).
     *
     * @return the number of entries that must be relocated to fill the node
     *         to a minimal level, or 0 if not possible.
     */
    private int tryNonLeafRelocateToFill(InnerPage page, InnerPage adjPage,
                                         boolean movingRight, int parentKeySize) {

        int adjKeys = adjPage.getNumKeys();
        int pageBytesFree = page.getFreeSpace();
        int adjBytesFree = adjPage.getFreeSpace();

        // Should be the same for both page and adjPage.
        int halfFull = page.getTotalSpace() / 2;

        // The parent key always has to move into this page, so if that
        // won't fit, don't even try.
        if (pageBytesFree < parentKeySize)
            return 0;

        pageBytesFree -= parentKeySize;

        int keyBytesMoved = 0;
        int lastKeySize = parentKeySize;

        int numRelocated = 0;
        while (true) {
            // If the key we wanted to move into this page overflows the free
            // space in this page, back it up.
            // TODO:  IS THIS NECESSARY?
            if (pageBytesFree < keyBytesMoved + 2 * numRelocated) {
                numRelocated--;
                break;
            }

            // Figure out the index of the key we need the size of, based on the
            // direction we are moving values.  If we are moving values right,
            // we need to look at the keys starting at the rightmost one.  If we
            // are moving values left, we need to start with the leftmost key.
            int index;
            if (movingRight)
                index = adjKeys - numRelocated - 1;
            else
                index = numRelocated;

            keyBytesMoved += lastKeySize;

            lastKeySize = adjPage.getKey(index).getSize();
            logger.debug("Key " + index + " is " + lastKeySize + " bytes");

            numRelocated++;

            // Since we don't yet know which page the new pointer will go into,
            // stop when we can put the pointer in either page.
            if (adjBytesFree <= halfFull &&
                (pageBytesFree + keyBytesMoved + 2 * numRelocated) <= halfFull) {
                break;
            }
        }

        logger.debug("Can relocate " + numRelocated +
                " keys to satisfy minimum space requirements.");

        assert numRelocated >= 0;
        return numRelocated;
    }
}
