package edu.caltech.nanodb.storage.btreefile;


import java.io.IOException;
import java.util.List;

import org.apache.log4j.Logger;

import edu.caltech.nanodb.expressions.TupleComparator;
import edu.caltech.nanodb.expressions.TupleLiteral;
import edu.caltech.nanodb.relations.Tuple;
import edu.caltech.nanodb.storage.DBFile;
import edu.caltech.nanodb.storage.DBPage;
import edu.caltech.nanodb.storage.StorageManager;


/**
 * This class provides high-level B<sup>+</sup> tree management operations
 * performed on leaf nodes.  These operations are provided here and not on the
 * {@link LeafPage} class since they sometimes involve splitting or merging
 * leaf nodes, updating parent nodes, and so forth.
 */
public class LeafPageOperations {
    /** A logging object for reporting anything interesting that happens. */
    private static Logger logger = Logger.getLogger(LeafPageOperations.class);

    private StorageManager storageManager;


    private BTreeTupleFile tupleFile;


    private FileOperations fileOps;

    private InnerPageOperations innerPageOps;


    public LeafPageOperations(StorageManager storageManager,
                              BTreeTupleFile tupleFile,
                              FileOperations fileOps,
                              InnerPageOperations innerPageOps) {
        this.storageManager = storageManager;
        this.tupleFile = tupleFile;
        this.fileOps = fileOps;
        this.innerPageOps = innerPageOps;
    }


    /**
     * This helper function provides the simple operation of loading a leaf page
     * from its page-number, or if the page-number is 0 then {@code null} is
     * returned.
     *
     * @param pageNo the page-number to load as a leaf-page.
     *
     * @return a newly initialized {@link LeafPage} instance if {@code pageNo}
     *         is positive, or {@code null} if {@code pageNo} is 0.
     *
     * @throws IOException if an IO error occurs while loading the specified
     *         page
     *
     * @throws IllegalArgumentException if the specified page isn't a leaf-page
     */
    private LeafPage loadLeafPage(int pageNo)
        throws IOException {

        if (pageNo == 0)
            return null;

        DBFile dbFile = tupleFile.getDBFile();
        DBPage dbPage = storageManager.loadDBPage(dbFile, pageNo);
        return new LeafPage(dbPage, tupleFile.getSchema());
    }


    /**
     * This helper function will handle deleting a tuple in the index.
     *
     * @param leaf the leaf page to delete the tuple from
     *
     * @param tuple the tuple to delete from the leaf page
     *
     * @param pagePath the path of pages taken from the root page to the leaf
     *        page, represented as a list of page numbers
     *
     * @throws IOException if an IO error occurs while updating the index
     */
    public void deleteTuple(LeafPage leaf, Tuple tuple,
                            List<Integer> pagePath) throws IOException {

        logger.debug(String.format("Deleting tuple %s from leaf page %d at " +
            "page-path %s", tuple, leaf.getPageNo(), pagePath));

        leaf.deleteTuple(tuple);

        if (leaf.getUsedSpace() >= leaf.getTotalSpace() / 2) {
            // The page is at least half-full.  Don't need to redistribute or
            // coalesce.
            return;
        }
        else if (pagePath.size() == 1) {
            // The page is the root.  Don't need to redistribute or coalesce,
            // but if the root is now empty, need to shorten the tree depth.
            // (Since this is a leaf page, the depth will go down to 0.)

            if (leaf.getNumTuples() == 0) {
                logger.debug(String.format("Current root page %d is now " +
                    "empty, removing.", leaf.getPageNo()));

                // Set the index's root page to 0 (empty) since the only page
                // in the index is now empty and being removed.
                DBPage dbpHeader =
                    storageManager.loadDBPage(tupleFile.getDBFile(), 0);
                HeaderPage.setRootPageNo(dbpHeader, 0);

                // Free up this page in the index.
                fileOps.releaseDataPage(leaf.getDBPage());

                if (tuple instanceof BTreeFilePageTuple) {
                    // Some sanity checks - if the btree is now empty,
                    // the "next tuple" info should be all 0.
                    BTreeFilePageTuple btpt = (BTreeFilePageTuple) tuple;
                    assert btpt.getNextTuplePageNo() == 0;
                    assert btpt.getNextTupleIndex() == 0;
                }
            }

            return;
        }

        // If we got to this part, we have to redistribute/coalesce stuff :(

        // Note: Assumed that at least one of the siblings has the same
        // immediate parent.  If that is not the case... we're doomed...
        // TODO:  VERIFY THIS AND THROW AN EXCEPTION IF NOT

        int leafPageNo = leaf.getPageNo();

        // Leaf pages know their right sibling, so that's why finding the
        // right page doesn't require the innerPageOps object.
        int leftPageNo = leaf.getLeftSibling(pagePath, innerPageOps);
        int rightPageNo = leaf.getRightSibling(pagePath);

        logger.debug(String.format("Leaf page %d is too empty.  Left " +
            "sibling is %d, right sibling is %d.", leafPageNo, leftPageNo,
            rightPageNo));

        if (leftPageNo == -1 && rightPageNo == -1) {
            // We should never get to this point, since the earlier test
            // should have caught this situation.
            throw new IllegalStateException(String.format(
                "Leaf node %d doesn't have a left or right sibling!",
                leaf.getPageNo()));
        }

        // Now we know that at least one sibling is present.  Load both
        // siblings and coalesce/redistribute in the direction that makes
        // the most sense...

        LeafPage leftSibling = null;
        if (leftPageNo != -1)
            leftSibling = loadLeafPage(leftPageNo);

        LeafPage rightSibling = null;
        if (rightPageNo != -1)
            rightSibling = loadLeafPage(rightPageNo);

        assert leftSibling != null || rightSibling != null;

        // See if we can coalesce the node into its left or right sibling.
        // When we do the check, we must not forget that each node contains a
        // header, and we need to account for that space as well.  This header
        // space is included in the getUsedSpace() method, but is excluded by
        // the getSpaceUsedByTuples() method.

        // TODO:  SEE IF WE CAN SIMPLIFY THIS AT ALL...
        if (leftSibling != null &&
            leftSibling.getUsedSpace() + leaf.getSpaceUsedByTuples() <
            leftSibling.getTotalSpace()) {

            // Coalesce the current node into the left sibling.
            logger.debug("Delete from leaf " + leaf.getPageNo() +
                ":  coalescing with left sibling leaf.");

            logger.debug(String.format("Before coalesce-left, page has %d " +
                "tuples and left sibling has %d tuples.",
                leaf.getNumTuples(), leftSibling.getNumTuples()));

            if (tuple instanceof BTreeFilePageTuple) {
                // The "next tuple" will end up in the left sibling, so we
                // need to update this info in the deleted tuple.
                BTreeFilePageTuple btpt = (BTreeFilePageTuple) tuple;
                int index = btpt.getNextTupleIndex();
                index += leftSibling.getNumTuples();
                btpt.setNextTuplePosition(leftPageNo, index);
            }

            leaf.moveTuplesLeft(leftSibling, leaf.getNumTuples());
            leftSibling.setNextPageNo(leaf.getNextPageNo());

            logger.debug(String.format("After coalesce-left, page has %d " +
                "tuples and left sibling has %d tuples.",
                leaf.getNumTuples(), leftSibling.getNumTuples()));

            // Free up the leaf page since it's empty now
            fileOps.releaseDataPage(leaf.getDBPage());

            // Since the leaf page has been removed from the index structure,
            // we need to remove it from the parent page.  Also, since the
            // page was coalesced into its left sibling, we need to remove
            // the tuple to the left of the pointer being removed.

            InnerPage parent =
                innerPageOps.loadPage(pagePath.get(pagePath.size() - 2));

            List<Integer> parentPagePath = pagePath.subList(0, pagePath.size() - 1);
            innerPageOps.deletePointer(parent, parentPagePath, leafPageNo,
                /* remove right tuple */ false);
        }
        else if (rightSibling != null &&
                 rightSibling.getUsedSpace() + leaf.getSpaceUsedByTuples() <
                 rightSibling.getTotalSpace()) {

            // Coalesce the current node into the right sibling.
            logger.debug("Delete from leaf " + leaf.getPageNo() +
                ":  coalescing with right sibling leaf.");

            logger.debug(String.format("Before coalesce-right, page has %d " +
                "tuples and right sibling has %d tuples.",
                leaf.getNumTuples(), rightSibling.getNumTuples()));

            if (tuple instanceof BTreeFilePageTuple) {
                // The "next tuple" will end up in the right sibling, so we
                // need to update this info in the deleted tuple.  We don't
                // update the index, because this page's tuples will precede
                // the right sibling's tuples.
                BTreeFilePageTuple btpt = (BTreeFilePageTuple) tuple;
                int index = btpt.getNextTupleIndex();
                btpt.setNextTuplePosition(rightPageNo, index);
            }

            leaf.moveTuplesRight(rightSibling, leaf.getNumTuples());

            // Left sibling can be null if leaf is the first leaf node in the
            // sequence of the btree.
            if (leftSibling != null)
                leftSibling.setNextPageNo(rightPageNo);

            logger.debug(String.format("After coalesce-right, page has %d " +
                "tuples and right sibling has %d tuples.",
                leaf.getNumTuples(), rightSibling.getNumTuples()));

            // Free up the leaf page since it's empty now
            fileOps.releaseDataPage(leaf.getDBPage());

            // Since the leaf page has been removed from the index structure,
            // we need to remove it from the parent page.  Also, since the
            // page was coalesced into its right sibling, we need to remove
            // the tuple to the right of the pointer being removed.

            InnerPage parent =
                innerPageOps.loadPage(pagePath.get(pagePath.size() - 2));

            List<Integer> parentPagePath = pagePath.subList(0, pagePath.size() - 1);
            innerPageOps.deletePointer(parent, parentPagePath, leafPageNo,
                /* remove right tuple */ true);
        }
        else {
            // Can't coalesce the leaf node into either sibling.  Redistribute
            // tuples from left or right sibling into the leaf.  The strategy
            // is as follows:

            // If the node has both left and right siblings, redistribute from
            // the fuller sibling.  Otherwise, just redistribute from
            // whichever sibling we have.

            LeafPage adjPage;
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

            int tuplesToMove = tryLeafRelocateToFill(leaf, adjPage,
                /* movingRight */ adjPage == leftSibling);

            if (tuplesToMove == 0) {
                // We really tried to satisfy the "minimum size" requirement,
                // but we just couldn't.  Log it and return.

                StringBuilder buf = new StringBuilder();

                buf.append(String.format("Couldn't relocate tuples to satisfy" +
                    " minimum space requirement in leaf-page %d with %d tuples!\n",
                    leaf.getPageNo(), leaf.getNumTuples()));

                if (leftSibling != null) {
                    buf.append(
                        String.format("\t- Left sibling page %d has %d tuples\n",
                        leftSibling.getPageNo(), leftSibling.getNumTuples()));
                }
                else {
                    buf.append("\t- No left sibling\n");
                }

                if (rightSibling != null) {
                    buf.append(
                        String.format("\t- Right sibling page %d has %d tuples",
                        rightSibling.getPageNo(), rightSibling.getNumTuples()));
                }
                else {
                    buf.append("\t- No right sibling");
                }

                logger.warn(buf);

                return;
            }

            logger.debug(String.format("Relocating %d tuples into leaf page " +
                "%d from %s sibling page %d", tuplesToMove, leaf.getPageNo(),
                (adjPage == leftSibling ? "left" : "right"), adjPage.getPageNo()));

            if (tuple instanceof BTreeFilePageTuple) {
                // Since we are moving tuples into this page, we may need to
                // modify the "next tuple" info.  This is only necessary if
                // tuples are moved from the left sibling, since those will
                // precede the leaf page's current tuples.
                BTreeFilePageTuple btpt = (BTreeFilePageTuple) tuple;
                int nextPageNo = btpt.getNextTuplePageNo();
                int nextIndex = btpt.getNextTupleIndex();

                if (adjPage == leftSibling) {
                    if (nextPageNo == leafPageNo) {
                        logger.debug(String.format("Moving %d tuples from left " +
                            "sibling %d to leaf %d.  Deleted tuple has next " +
                            "tuple at [%d:%d]; updating to [%d:%d]", tuplesToMove,
                            leftPageNo, leafPageNo, nextPageNo, nextIndex,
                            nextPageNo, nextIndex + tuplesToMove));

                        btpt.setNextTuplePosition(nextPageNo, nextIndex + tuplesToMove);
                    }
                    else {
                        assert(nextIndex == 0);

                        logger.debug(String.format("Moving %d tuples from " +
                            "left sibling %d to leaf %d.  Deleted tuple " +
                            "has next tuple at [%d:%d]; not updating",
                            tuplesToMove, leftPageNo, leafPageNo,
                            nextPageNo, nextIndex));
                    }
                }
                else {
                    assert adjPage == rightSibling;

                    if (nextPageNo == rightPageNo) {
                        assert(nextIndex == 0);

                        logger.debug(String.format("Moving %d tuples from " +
                            "right sibling %d to leaf %d.  Deleted tuple " +
                            "has next tuple at [%d:%d]; not updating",
                            tuplesToMove, rightPageNo, leafPageNo,
                            nextPageNo, nextIndex));

                        btpt.setNextTuplePosition(leafPageNo, leaf.getNumTuples());
                    }
                    else {
                        logger.debug(String.format("Moving %d tuples from " +
                            "right sibling %d to leaf %d.  Deleted tuple " +
                            "has next tuple at [%d:%d]; not updating",
                            tuplesToMove, rightPageNo, leafPageNo,
                            nextPageNo, nextIndex));
                    }
                }
            }

            InnerPage parent =
                innerPageOps.loadPage(pagePath.get(pagePath.size() - 2));
            int index;

            if (adjPage == leftSibling) {
                adjPage.moveTuplesRight(leaf, tuplesToMove);
                index = parent.getIndexOfPointer(adjPage.getPageNo());
                parent.replaceTuple(index, leaf.getTuple(0));
            }
            else { // adjPage == right sibling
                adjPage.moveTuplesLeft(leaf, tuplesToMove);
                index = parent.getIndexOfPointer(leaf.getPageNo());
                parent.replaceTuple(index, adjPage.getTuple(0));
            }
        }
    }


    /**
     * This helper function handles the operation of adding a new tuple to a
     * leaf-page of the index.  This operation is provided here and not on the
     * {@link LeafPage} class, because adding the new tuple might require the
     * leaf page to be split into two pages.
     *
     * @param leaf the leaf page to add the tuple to
     *
     * @param newTuple the new tuple to add to the leaf page
     *
     * @param pagePath the path of pages taken from the root page to this leaf
     *        page, represented as a list of page numbers in the data file
     *
     * @throws IOException if an IO error occurs while updating the index
     */
    public BTreeFilePageTuple addTuple(LeafPage leaf, TupleLiteral newTuple,
        List<Integer> pagePath) throws IOException {

        BTreeFilePageTuple result;

        // Figure out where the new tuple-value goes in the leaf page.

        int newTupleSize = newTuple.getStorageSize();
        if (leaf.getFreeSpace() < newTupleSize) {
            // Try to relocate tuples from this leaf to either sibling,
            // or if that can't happen, split the leaf page into two.
            result = relocateTuplesAndAddTuple(leaf, pagePath, newTuple);
            if (result == null)
                result = splitLeafAndAddTuple(leaf, pagePath, newTuple);
        }
        else {
            // There is room in the leaf for the new tuple.  Add it there.
            result = leaf.addTuple(newTuple);
        }

        return result;
    }


    /**
     * This method attempts to relocate tuples to the left or right sibling
     * of the specified node, and then insert the specified tuple into the
     * appropriate node.  If it's not possible to relocate tuples (perhaps
     * because there isn't space and/or because there is no sibling), the
     * method returns {@code null}.
     *
     * @param page The leaf page to relocate tuples out of.
     *
     * @param pagePath The path from the index's root to the leaf page
     *
     * @param tuple The tuple to add to the index.
     *
     * @return a {@code BTreeFilePageTuple} representing the actual tuple
     *         added into the tree structure, or {@code null} if tuples
     *         couldn't be relocated to make space for the new tuple.
     *
     * @throws IOException if an IO error occurs while attempting to retrieve
     *         the page's siblings.
     */
    private BTreeFilePageTuple relocateTuplesAndAddTuple(LeafPage page,
        List<Integer> pagePath, TupleLiteral tuple) throws IOException {

        // See if we are able to relocate records either direction to free up
        // space for the new tuple.

        int bytesRequired = tuple.getStorageSize();

        int pathSize = pagePath.size();
        if (pathSize == 1)  // This node is also the root - no parent.
            return null;    // There aren't any siblings to relocate to.

        if (pagePath.get(pathSize - 1) != page.getPageNo()) {
            throw new IllegalArgumentException(
                "leaf page number doesn't match last page-number in page path");
        }

        int parentPageNo = 0;
        if (pathSize >= 2)
            parentPageNo = pagePath.get(pathSize - 2);

        InnerPage parentPage = innerPageOps.loadPage(parentPageNo);
        int numPointers = parentPage.getNumPointers();
        int pagePtrIndex = parentPage.getIndexOfPointer(page.getPageNo());

        // Check each sibling in its own code block so that we can constrain
        // the scopes of the variables a bit.  This keeps us from accidentally
        // reusing the "prev" variables in the "next" section.

        {
            LeafPage prevPage = null;
            if (pagePtrIndex - 1 >= 0)
                prevPage = loadLeafPage(parentPage.getPointer(pagePtrIndex - 1));

            if (prevPage != null) {
                // See if we can move some of this leaf's tuples to the
                // previous leaf, to free up space.

                int count = tryLeafRelocateForSpace(page, prevPage, false,
                    bytesRequired);

                if (count > 0) {
                    // Yes, we can do it!
                    logger.debug(String.format("Relocating %d tuples from " +
                        "leaf-page %d to left-sibling leaf-page %d", count,
                        page.getPageNo(), prevPage.getPageNo()));

                    logger.debug("Space before relocation:  Leaf = " +
                        page.getFreeSpace() + " bytes\t\tSibling = " +
                        prevPage.getFreeSpace() + " bytes");

                    page.moveTuplesLeft(prevPage, count);

                    logger.debug("Space after relocation:  Leaf = " +
                        page.getFreeSpace() + " bytes\t\tSibling = " +
                        prevPage.getFreeSpace() + " bytes");

                    BTreeFilePageTuple result =
                        addTupleToLeafPair(prevPage, page, tuple);

                    if (result == null) {
                        // Even with relocating tuples, we couldn't free up
                        // enough space.  :-(
                        return null;
                    }

                    // Since we relocated tuples between two nodes, update
                    // the parent page to reflect the tuple that is now at
                    // the start of the right page.
                    BTreeFilePageTuple firstRightTuple = page.getTuple(0);
                    pagePath.remove(pathSize - 1);
                    innerPageOps.replaceTuple(parentPage, pagePath,
                        prevPage.getPageNo(), firstRightTuple, page.getPageNo());

                    return result;
                }
            }
        }

        {
            LeafPage nextPage = null;
            if (pagePtrIndex + 1 < numPointers)
                nextPage = loadLeafPage(parentPage.getPointer(pagePtrIndex + 1));

            if (nextPage != null) {
                // See if we can move some of this leaf's tuples to the next
                // leaf, to free up space.

                int count = tryLeafRelocateForSpace(page, nextPage, true,
                    bytesRequired);

                if (count > 0) {
                    // Yes, we can do it!

                    logger.debug(String.format("Relocating %d tuples from " +
                        "leaf-page %d to right-sibling leaf-page %d", count,
                        page.getPageNo(), nextPage.getPageNo()));

                    logger.debug("Space before relocation:  Leaf = " +
                        page.getFreeSpace() + " bytes\t\tSibling = " +
                        nextPage.getFreeSpace() + " bytes");

                    page.moveTuplesRight(nextPage, count);

                    logger.debug("Space after relocation:  Leaf = " +
                        page.getFreeSpace() + " bytes\t\tSibling = " +
                        nextPage.getFreeSpace() + " bytes");

                    BTreeFilePageTuple result =
                        addTupleToLeafPair(page, nextPage, tuple);

                    if (result == null) {
                        // Even with relocating tuples, we couldn't free up
                        // enough space.  :-(
                        return null;
                    }

                    // Since we relocated tuples between two nodes, update
                    // the parent page to reflect the tuple that is now at
                    // the start of the right page.
                    BTreeFilePageTuple firstRightTuple = nextPage.getTuple(0);
                    pagePath.remove(pathSize - 1);
                    innerPageOps.replaceTuple(parentPage, pagePath,
                        page.getPageNo(), firstRightTuple, nextPage.getPageNo());

                    return result;
                }
            }
        }

        // Couldn't relocate tuples to either the previous or next page.  We
        // must split the leaf into two.
        return null;
    }


    /**
     * This helper method takes a pair of leaf nodes that are siblings to each
     * other, and adds the specified tuple to whichever leaf the tuple should
     * go into.  The method returns the {@code BTreeFilePageTuple} object
     * representing the actual tuple in the tree once it has been added.
     *
     * @param prevLeaf the first leaf in the pair, left sibling of
     *        {@code nextLeaf}
     *
     * @param nextLeaf the second leaf in the pair, right sibling of
     *        {@code prevLeaf}
     *
     * @param tuple the tuple to insert into the pair of leaves
     *
     * @return the actual tuple in the page, after the insert is completed
     */
    private BTreeFilePageTuple addTupleToLeafPair(LeafPage prevLeaf,
        LeafPage nextLeaf, TupleLiteral tuple) {

        BTreeFilePageTuple result = null;
        BTreeFilePageTuple firstRightTuple = nextLeaf.getTuple(0);
        if (TupleComparator.compareTuples(tuple, firstRightTuple) < 0) {
            // The new tuple goes in the left page.  Hopefully there is room
            // for it...
            logger.debug("Adding tuple to left leaf " + prevLeaf.getPageNo() +
                " in pair");
            if (prevLeaf.getFreeSpace() >= tuple.getStorageSize())
                result = prevLeaf.addTuple(tuple);
        }
        else {
            // The new tuple goes in the right page.  Again, hopefully there
            // is room for it...
            logger.debug("Adding tuple to right leaf " + nextLeaf.getPageNo() +
                " in pair");
            if (nextLeaf.getFreeSpace() >= tuple.getStorageSize())
                result = nextLeaf.addTuple(tuple);
        }

        return result;
    }


    /**
     * This helper function determines how many tuples must be relocated from
     * one leaf-page to another, in order to free up the specified number of
     * bytes.  If it is possible, the number of tuples that must be relocated
     * is returned.  If it is not possible, the method returns 0.
     *
     * @param leaf the leaf node to relocate tuples from
     *
     * @param adjLeaf the adjacent leaf (predecessor or successor) to relocate
     *        tuples to
     *
     * @param movingRight pass {@code true} if the sibling is to the right of
     *        {@code page} (and therefore we are moving tuples right), or
     *        {@code false} if the sibling is to the left of {@code page} (and
     *        therefore we are moving tuples left).
     *
     * @param bytesRequired the number of bytes that must be freed up in
     *        {@code leaf} by the operation
     *
     * @return the number of tuples that must be relocated to free up the
     *         required space, or 0 if it is not possible.
     */
    private int tryLeafRelocateForSpace(LeafPage leaf, LeafPage adjLeaf,
        boolean movingRight, int bytesRequired) {

        int numTuples = leaf.getNumTuples();
        int leafBytesFree = leaf.getFreeSpace();
        int adjBytesFree = adjLeaf.getFreeSpace();

        logger.debug("Leaf bytes free:  " + leafBytesFree +
            "\t\tAdjacent leaf bytes free:  " + adjBytesFree);

        // Subtract the bytes-required from the adjacent-bytes-free value so
        // that we ensure we always have room to put the tuple in either node.
        adjBytesFree -= bytesRequired;

        int numRelocated = 0;
        while (true) {
            // Figure out the index of the tuple we need the size of, based on
            // the direction we are moving values.  If we are moving values
            // right, we need to look at the tuples starting at the rightmost
            // one.  If we are moving tuples left, we need to start with the
            // leftmost tuple.
            int index;
            if (movingRight)
                index = numTuples - numRelocated - 1;
            else
                index = numRelocated;

            int tupleSize = leaf.getTupleSize(index);

            logger.debug("Tuple " + index + " is " + tupleSize + " bytes");

            // Did we run out of space to move tuples before we hit our goal?
            if (adjBytesFree < tupleSize) {
                numRelocated = 0;
                break;
            }

            numRelocated++;

            leafBytesFree += tupleSize;
            adjBytesFree -= tupleSize;

            // Since we don't yet know which leaf the new tuple will go into,
            // stop when we can put the tuple in either leaf.
            if (leafBytesFree >= bytesRequired &&
                adjBytesFree >= bytesRequired) {
                break;
            }
        }

        logger.debug("Can relocate " + numRelocated + " tuples to free up space.");

        return numRelocated;
    }


    /**
     * This helper function splits the specified leaf-node into two nodes,
     * also updating the parent node in the process, and then inserts the
     * specified tuple into the appropriate leaf.  This method is used to add
     * a tuple to a leaf that doesn't have enough space, when it isn't
     * possible to relocate values to the left or right sibling of the leaf.
     *
     * @todo (donnie) When the leaf node is split, half of the tuples are
     *       put into the new leaf, regardless of the size of individual
     *       tuples.  In other words, this method doesn't try to keep the
     *       leaves half-full based on bytes used.  It would almost
     *       certainly be better if it did.
     *
     * @param leaf the leaf node to split and then add the tuple to
     * @param pagePath the sequence of page-numbers traversed to reach this
     *        leaf node.
     *
     * @param tuple the new tuple to insert into the leaf node
     *
     * @throws IOException if an IO error occurs during the operation.
     */
    private BTreeFilePageTuple splitLeafAndAddTuple(LeafPage leaf,
        List<Integer> pagePath, TupleLiteral tuple) throws IOException {

        int pathSize = pagePath.size();
        if (pagePath.get(pathSize - 1) != leaf.getPageNo()) {
            throw new IllegalArgumentException(
                "Leaf page number doesn't match last page-number in page path");
        }

        if (logger.isDebugEnabled()) {
            logger.debug("Splitting leaf-page " + leaf.getPageNo() +
                " into two leaves.");
            logger.debug("    Old next-page:  " + leaf.getNextPageNo());
        }

        // Get a new blank page in the index, with the same parent as the
        // leaf-page we were handed.

        DBPage newDBPage = fileOps.getNewDataPage();
        LeafPage newLeaf = LeafPage.init(newDBPage, tupleFile.getSchema());

        /* TODO:  IMPLEMENT THE REST OF THIS METHOD.
         *
         * The LeafPage class provides some helpful operations for moving leaf-
         * entries to a left or right sibling.
         *
         * The parent page must also be updated.  If the leaf node doesn't have
         * a parent, the tree's depth will increase by one level.
         */
        logger.error("NOT YET IMPLEMENTED:  splitLeafAndAddKey()");
        return null;
    }


    /**
     * This helper function determines how many tuples must be relocated from
     * one leaf-page to another, in order to satisfy the "minimum space"
     * requirement of the B tree.free up the specified number of
     * bytes.  If it is possible, the number of tuples that must be relocated
     * is returned.  If it is not possible, the method returns 0.
     *
     * @param leaf the leaf node to relocate tuples from
     *
     * @param adjLeaf the adjacent leaf (predecessor or successor) to relocate
     *        tuples to
     *
     * @param movingRight pass {@code true} if the sibling is to the left of
     *        {@code page} (and therefore we are moving tuples right), or
     *        {@code false} if the sibling is to the right of {@code page}
     *        (and therefore we are moving tuples left).
     *
     * @return the number of tuples that must be relocated to fill the node
     *         to a minimal level, or 0 if not possible.
     */
    private int tryLeafRelocateToFill(LeafPage leaf, LeafPage adjLeaf,
                                      boolean movingRight) {

        int adjTuples = adjLeaf.getNumTuples();    // Tuples available to move
        int leafBytesFree = leaf.getFreeSpace();
        int adjBytesFree = adjLeaf.getFreeSpace();

        // Should be the same for both leaf and adjLeaf.
        int halfFull = leaf.getTotalSpace() / 2;

        logger.debug("Leaf bytes free:  " + leafBytesFree +
            "\t\tAdjacent leaf bytes free:  " + adjBytesFree);

        int numRelocated = 0;
        while (true) {
            // Figure out the index of the tuple we need the size of, based on
            // the direction we are moving values.  If we are moving values
            // right, we need to look at the tuples starting at the rightmost
            // one.  If we are moving values left, we need to start with the
            // leftmost tuple.
            int index;
            if (movingRight)
                index = adjTuples - numRelocated - 1;
            else
                index = numRelocated;

            int tupleSize = adjLeaf.getTupleSize(index);

            logger.debug("Tuple " + index + " is " + tupleSize + " bytes");

            // If we don't have room to move the adjacent node's tuple into
            // this node (unlikely), just stop there.
            if (leafBytesFree < tupleSize)
                break;

            // If the adjacent leaf would become too empty, stop relocating.
            if (adjBytesFree > halfFull)
                break;

            numRelocated++;

            leafBytesFree -= tupleSize;
            adjBytesFree += tupleSize;

            // Stop if the leaf now has at least the minimal number of bytes.
            if (leafBytesFree <= halfFull)
                break;
        }

        logger.debug("Can relocate " + numRelocated +
            " tuples to satisfy minimum space requirements.");

        return numRelocated;
    }
}
