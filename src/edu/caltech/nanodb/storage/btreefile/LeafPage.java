package edu.caltech.nanodb.storage.btreefile;


import java.io.IOException;

import java.util.ArrayList;
import java.util.List;

import edu.caltech.nanodb.relations.Schema;
import edu.caltech.nanodb.relations.Tuple;
import org.apache.log4j.Logger;

import edu.caltech.nanodb.expressions.TupleComparator;
import edu.caltech.nanodb.expressions.TupleLiteral;
import edu.caltech.nanodb.storage.DBPage;
import edu.caltech.nanodb.storage.PageTuple;

import static edu.caltech.nanodb.storage.btreefile.BTreePageTypes.*;


/**
 * <p>
 * This class wraps a {@link DBPage} object that is a leaf page in the
 * B<sup>+</sup> tree file implementation, to provide some of the basic
 * leaf-management operations necessary for the file structure.
 * </p>
 * <p>
 * Operations involving individual inner-pages are provided by the
 * {@link InnerPage} wrapper-class.  Higher-level operations involving
 * multiple leaves and/or inner pages of the B<sup>+</sup> tree structure,
 * are provided by the {@link LeafPageOperations} and
 * {@link InnerPageOperations} classes.
 * </p>
 */
public class LeafPage implements DataPage {
    /** A logging object for reporting anything interesting that happens. */
    private static Logger logger = Logger.getLogger(LeafPage.class);


    /**
     * The offset where the next-sibling page number is stored in this page.
     * The only leaf page that doesn't have a next sibling is the last leaf
     * in the file; its "next page" value will be set to 0.
     */
    public static final int OFFSET_NEXT_PAGE_NO = 1;


    /**
     * The offset where the number of tuples is stored in the page.
     */
    public static final int OFFSET_NUM_TUPLES = 3;


    /** The offset of the first tuple in the leaf page. */
    public static final int OFFSET_FIRST_TUPLE = 5;


    /** The actual data page that holds the B<sup>+</sup> tree leaf node. */
    private DBPage dbPage;


    /** The schema of the tuples in the leaf page. */
    private Schema schema;


    /** The number of tuples stored within this leaf page. */
    private int numTuples;


    /** A list of the tuples stored in this leaf page. */
    private ArrayList<BTreeFilePageTuple> tuples;


    /**
     * The total size of all data (tuples + initial values) stored within this
     * leaf page.  This is also the offset at which we can start writing more
     * data without overwriting anything.
     */
    private int endOffset;


    /**
     * Initialize the leaf-page wrapper class for the specified B<sup>+</sup>
     * tree leaf page.  The contents of the leaf-page are cached in the fields
     * of the wrapper object.
     *
     * @param dbPage the data page from the B<sup>+</sup> Tree file to wrap
     * @param schema the schema of tuples stored in the data page
     */
    public LeafPage(DBPage dbPage, Schema schema) {
        if (dbPage.readUnsignedByte(0) != BTREE_LEAF_PAGE) {
            throw new IllegalArgumentException("Specified DBPage " +
                dbPage.getPageNo() + " is not marked as a leaf page.");
        }

        this.dbPage = dbPage;
        this.schema = schema;

        loadPageContents();
    }


    /**
     * This static helper function initializes a {@link DBPage} object's
     * contents with the type and detail values that will allow a new
     * {@code LeafPage} wrapper to be instantiated for the page, and then it
     * returns a wrapper object for the page.
     *
     * @param dbPage the page to initialize as a leaf page.
     *
     * @param schema the schema of the tuples in the leaf page
     *
     * @return a newly initialized {@code LeafPage} object wrapping the page
     */
    public static LeafPage init(DBPage dbPage, Schema schema) {
        dbPage.writeByte(OFFSET_PAGE_TYPE, BTREE_LEAF_PAGE);
        dbPage.writeShort(OFFSET_NUM_TUPLES, 0);
        dbPage.writeShort(OFFSET_NEXT_PAGE_NO, 0);

        return new LeafPage(dbPage, schema);
    }


    /**
     * This private helper scans through the leaf page's contents and caches
     * the contents of the leaf page in a way that makes it easy to use and
     * manipulate.
     */
    private void loadPageContents() {
        numTuples = dbPage.readUnsignedShort(OFFSET_NUM_TUPLES);
        tuples = new ArrayList<BTreeFilePageTuple>(numTuples);

        if (numTuples > 0) {
            // Handle first tuple separately since we know its offset.

            BTreeFilePageTuple tuple =
                new BTreeFilePageTuple(schema, dbPage, OFFSET_FIRST_TUPLE, 0);

            tuples.add(tuple);

            // Handle remaining tuples.
            for (int i = 1; i < numTuples; i++) {
                int tupleEndOffset = tuple.getEndOffset();
                tuple = new BTreeFilePageTuple(schema, dbPage, tupleEndOffset, i);
                tuples.add(tuple);
            }

            endOffset = tuple.getEndOffset();
        }
        else {
            // There are no tuples in the leaf page.
            endOffset = OFFSET_FIRST_TUPLE;
        }
    }


    /**
     * Returns the schema of tuples in this page.
     *
     * @return the schema of tuples in this page
     */
    public Schema getSchema() {
        return schema;
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
     * Returns the page-number of the next leaf page in the sequence of leaf
     * pages, or 0 if this is the last leaf-page in the B<sup>+</sup> tree
     * file.
     *
     * @return the page-number of the next leaf page in the sequence of leaf
     *         pages, or 0 if this is the last leaf-page in the B<sup>+</sup>
     *         tree file.
     */
    public int getNextPageNo() {
        return dbPage.readUnsignedShort(OFFSET_NEXT_PAGE_NO);
    }


    /**
     * Sets the page-number of the next leaf page in the sequence of leaf pages.
     *
     * @param pageNo the page-number of the next leaf-page in the index, or 0
     *        if this is the last leaf-page in the B<sup>+</sup> tree file.
     */
    public void setNextPageNo(int pageNo) {
        if (pageNo < 0) {
            throw new IllegalArgumentException(
                "pageNo must be in range [0, 65535]; got " + pageNo);
        }

        dbPage.writeShort(OFFSET_NEXT_PAGE_NO, pageNo);
    }


    /**
     * Returns the number of tuples in this leaf-page.  Note that this count
     * does not include the pointer to the next leaf; it only includes the
     * tuples themselves.
     *
     * @return the number of entries in this leaf-page.
     */
    public int getNumTuples() {
        return numTuples;
    }


    /**
     * Returns the amount of space currently used in this leaf page, in bytes.
     *
     * @return the amount of space currently used in this leaf page, in bytes.
     */
    public int getUsedSpace() {
        return endOffset;
    }


    /**
     * Returns the amount of space used by tuples in this page, in bytes.
     *
     * @return the amount of space used by tuples in this page, in bytes.
     */
    public int getSpaceUsedByTuples() {
        return endOffset - OFFSET_FIRST_TUPLE;
    }

    /**
     * Returns the amount of space available in this leaf page, in bytes.
     *
     * @return the amount of space available in this leaf page, in bytes.
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
     * Returns the tuple at the specified index.
     *
     * @param index the index of the tuple to retrieve
     *
     * @return the tuple at that index
     */
    public BTreeFilePageTuple getTuple(int index) {
        return tuples.get(index);
    }


    /**
     * Returns the size of the tuple at the specified index, in bytes.
     *
     * @param index the index of the tuple to get the size of
     *
     * @return the size of the specified tuple, in bytes
     */
    public int getTupleSize(int index) {
        BTreeFilePageTuple tuple = getTuple(index);
        return tuple.getEndOffset() - tuple.getOffset();
    }


    /**
     * Given a leaf page in the B<sup>+</sup> tree file, returns the page
     * number of the left sibling, or -1 if there is no left sibling to this
     * node.
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
     * Given a leaf page in the B<sup>+</sup> tree file, returns the page
     * number of the right sibling, or -1 if there is no right sibling to
     * this node.
     *
     * @param pagePath the page path from root to this leaf page
     *
     * @return the page number of the right sibling leaf-node, or -1 if there
     *         is no right sibling
     */
    public int getRightSibling(List<Integer> pagePath) throws IOException {

        // Verify that the last node in the page path is in fact this page.
        if (pagePath.get(pagePath.size() - 1) != getPageNo()) {
            throw new IllegalArgumentException(
                "The page path provided does not terminate on this leaf page.");
        }

        int rightSiblingPageNo = getNextPageNo();
        if (rightSiblingPageNo == 0)
            rightSiblingPageNo = -1;

        return rightSiblingPageNo;
    }


    /**
     * Returns the index of the specified tuple.
     *
     * @param tuple the tuple to retrieve the index for
     *
     * @return the integer index of the specified tuple, -1 if the tuple
     *         isn't in the page.
     */
    public int getTupleIndex(Tuple tuple) {
        int i;
        for (i = 0; i < numTuples; i++) {
            BTreeFilePageTuple pageTuple = tuples.get(i);

            /* This gets REALLY verbose... */
            logger.trace(i + ":  comparing " + tuple + " to " + pageTuple);

            // Is this the key we're looking for?
            if (TupleComparator.comparePartialTuples(tuple, pageTuple) == 0) {
                logger.debug(String.format("Found tuple:  %s  is equal to " +
                                               "%s at index %d (size = %d bytes)", tuple, pageTuple, i,
                                              pageTuple.getSize()));

                return i;
            }
        }
        return -1;
    }


    /**
     * This method will delete a tuple from the leaf page.  The method takes
     * care of 'sliding' the remaining data to cover up the gap left.  The
     * method throws an exception if the specified tuple does not appear in
     * the leaf page.
     *
     * @param tuple the tuple to delete from the leaf page
     *
     * @throws IllegalStateException if the specified tuple doesn't exist
     */
    public void deleteTuple(Tuple tuple) {
        logger.debug("Trying to delete tuple " + tuple + " from leaf page " +
            getPageNo());

        int index = getTupleIndex(tuple);
        if (index == -1) {
            throw new IllegalArgumentException("Specified tuple " + tuple +
                " does not appear in leaf page " + getPageNo());
        }

        int tupleOffset = getTuple(index).getOffset();
        int len = getTupleSize(index);

        logger.debug("Moving leaf-page data in range [" + (tupleOffset+len) +
             ", " + endOffset + ") over by " + len + " bytes");
        dbPage.moveDataRange(tupleOffset + len, tupleOffset,
                             endOffset - tupleOffset - len);

        // Decrement the total number of entries.
        dbPage.writeShort(OFFSET_NUM_TUPLES, numTuples - 1);

        logger.debug("Loading altered page - had " + numTuples +
            " tuples before delete.");
        // Load new page.
        loadPageContents();

        logger.debug("After loading, have " + numTuples + " tuples");

        if (tuple instanceof BTreeFilePageTuple) {
            BTreeFilePageTuple btpt = (BTreeFilePageTuple) tuple;
            btpt.setDeleted();

            if (index < numTuples)
                btpt.setNextTuplePosition(dbPage.getPageNo(), index);
            else
                btpt.setNextTuplePosition(getNextPageNo(), 0);
        }
    }


    /**
     * This method inserts a tuple into the leaf page, making sure to keep
     * tuples in monotonically increasing order.  This method will throw an
     * exception if the leaf page already contains the specified tuple.
     *
     * @param newTuple the new tuple to add to the leaf page
     *
     * @throws IllegalStateException if the specified tuple already appears in
     *         the leaf page.
     */
    public BTreeFilePageTuple addTuple(TupleLiteral newTuple) {
        if (newTuple.getStorageSize() == -1) {
            throw new IllegalArgumentException("New tuple's storage size " +
                "must be computed before this method is called.");
        }

        if (getFreeSpace() < newTuple.getStorageSize()) {
            throw new IllegalArgumentException(String.format(
                "Not enough space in this node to store the new tuple " +
                "(%d bytes free; %d bytes required)", getFreeSpace(),
                newTuple.getStorageSize()));
        }

        BTreeFilePageTuple result = null;

        if (numTuples == 0) {
            logger.debug("Leaf page is empty; storing new tuple at start.");
            result = addTupleAtIndex(newTuple, 0);
        }
        else {
            int i;
            for (i = 0; i < numTuples; i++) {
                BTreeFilePageTuple tuple = tuples.get(i);

                /* This gets REALLY verbose... */
                logger.trace(i + ":  comparing " + newTuple + " to " + tuple);

                // Compare the new tuple to the current tuple.  Once we find
                // where the new tuple should go, copy the tuple into the page.
                int cmp = TupleComparator.compareTuples(newTuple, tuple);
                if (cmp < 0) {
                    logger.debug("Storing new tuple at index " + i +
                        " in the leaf page.");
                    result = addTupleAtIndex(newTuple, i);
                    break;
                }
                else if (cmp == 0) {
                    // TODO:  Currently we require all tuples to be unique,
                    //        but this isn't a realistic long-term constraint.
                    throw new IllegalStateException("Tuple " + newTuple +
                        " already appears in the index!");
                }
            }

            if (i == numTuples) {
                // The new tuple will go at the end of this page's entries.
                logger.debug("Storing new tuple at end of leaf page.");
                result = addTupleAtIndex(newTuple, numTuples);
            }
        }

        // The addTupleAtIndex() method updates the internal fields that cache
        // where keys live, etc.  So, we don't need to do that here.

        assert result != null;  // Shouldn't be possible at this point...
        return result;
    }


    /**
     * This private helper takes care of inserting a tuple at a specific index
     * in the leaf page.  This method should be called with care, so as to
     * ensure that tuples always remain in monotonically increasing order.
     *
     * @param newTuple the new tuple to insert into the leaf page
     * @param index the index to insert the tuple at.  Any existing tuples at
     *        or after this index will be shifted over to make room for the
     *        new tuple.
     */
    private BTreeFilePageTuple addTupleAtIndex(TupleLiteral newTuple,
                                               int index) {

        logger.debug("Leaf-page is starting with data ending at index " +
            endOffset + ", and has " + numTuples + " tuples.");

        // Get the storage size of the new tuple.
        int len = newTuple.getStorageSize();
        if (len == -1) {
            throw new IllegalArgumentException("New tuple's storage size " +
                "must be computed before this method is called.");
        }

        logger.debug("New tuple's storage size is " + len + " bytes");

        int tupleOffset;
        if (index < numTuples) {
            // Need to slide tuples after this index over, to make space.

            BTreeFilePageTuple tuple = getTuple(index);

            // Make space for the new tuple to be stored, then copy in
            // the new values.

            tupleOffset = tuple.getOffset();

            logger.debug("Moving leaf-page data in range [" + tupleOffset +
                ", " + endOffset + ") over by " + len + " bytes");

            dbPage.moveDataRange(tupleOffset, tupleOffset + len,
                                 endOffset - tupleOffset);
        }
        else {
            // The new tuple falls at the end of the data in the leaf index
            // page.
            tupleOffset = endOffset;
            logger.debug("New tuple is at end of leaf-page data; not " +
                         "moving anything.");
        }

        // Write the tuple value into the page.
        PageTuple.storeTuple(dbPage, tupleOffset, schema, newTuple);

        // Increment the total number of tuples.
        dbPage.writeShort(OFFSET_NUM_TUPLES, numTuples + 1);

        // Reload the page contents now that we have a new tuple in the mix.
        // TODO:  We could do this more efficiently, but this should be
        //        sufficient for now.
        loadPageContents();

        logger.debug("Wrote new tuple to leaf-page at offset " + tupleOffset +
                     ".");
        logger.debug("Leaf-page is ending with data ending at index " +
            endOffset + ", and has " + numTuples + " tuples.");

        // Return the actual tuple we just added to the page.
        return getTuple(index);
    }


    /**
     * This helper function moves the specified number of tuples to the left
     * sibling of this leaf node.  The data is copied in one shot so that the
     * transfer will be fast, and the various associated bookkeeping values in
     * both leaves are updated.
     *
     * @param leftSibling the left sibling of this leaf-node in the
     *        B<sup>+</sup> tree file
     *
     * @param count the number of tuples to move to the left sibling
     */
    public void moveTuplesLeft(LeafPage leftSibling, int count) {
        if (leftSibling == null)
            throw new IllegalArgumentException("leftSibling cannot be null");

        if (leftSibling.getNextPageNo() != getPageNo()) {
            logger.error(String.format("Left sibling leaf %d says that " +
                "page %d is its right sibling, not this page %d",
                leftSibling.getPageNo(), leftSibling.getNextPageNo(),
                 getPageNo()));

            throw new IllegalArgumentException("leftSibling " +
                leftSibling.getPageNo() + " isn't actually the left " +
                "sibling of this leaf-node " + getPageNo());
        }

        if (count < 0 || count > numTuples) {
            throw new IllegalArgumentException("count must be in range [0, " +
                numTuples + "), got " + count);
        }

        int moveEndOffset = getTuple(count - 1).getEndOffset(); //getTuple(count).getOffset()
        int len = moveEndOffset - OFFSET_FIRST_TUPLE;

        // Copy the range of tuple-data to the destination page.  Then update
        // the count of tuples in the destination page.
        // Don't need to move any data in the left sibling; we are appending!
        leftSibling.dbPage.write(leftSibling.endOffset, dbPage.getPageData(),
            OFFSET_FIRST_TUPLE, len);          // Copy the tuple-data across
        leftSibling.dbPage.writeShort(OFFSET_NUM_TUPLES,
            leftSibling.numTuples + count);    // Update the tuple-count

        // Remove that range of tuple-data from this page.
        dbPage.moveDataRange(moveEndOffset, OFFSET_FIRST_TUPLE,
            endOffset - moveEndOffset);
        dbPage.writeShort(OFFSET_NUM_TUPLES, numTuples - count);

        // Only erase the old data in the leaf page if we are trying to make
        // sure everything works properly.
        if (BTreeTupleFile.CLEAR_OLD_DATA)
            dbPage.setDataRange(endOffset - len, len, (byte) 0);

        // Update the cached info for both leaves.
        loadPageContents();
        leftSibling.loadPageContents();
    }


    /**
     * This helper function moves the specified number of tuples to the right
     * sibling of this leaf node.  The data is copied in one shot so that the
     * transfer will be fast, and the various associated bookkeeping values in
     * both leaves are updated.
     *
     * @param rightSibling the right sibling of this leaf-node in the index
     *        file
     *
     * @param count the number of tuples to move to the right sibling
     */
    public void moveTuplesRight(LeafPage rightSibling, int count) {
        if (rightSibling == null)
            throw new IllegalArgumentException("rightSibling cannot be null");

        if (getNextPageNo() != rightSibling.getPageNo()) {
            throw new IllegalArgumentException("rightSibling " +
                rightSibling.getPageNo() + " isn't actually the right " +
                "sibling of this leaf-node " + getPageNo());
        }

        if (count < 0 || count > numTuples) {
            throw new IllegalArgumentException("count must be in range [0, " +
                numTuples + "), got " + count);
        }

        int startOffset = getTuple(numTuples - count).getOffset();
        int len = endOffset - startOffset;

        // Copy the range of tuple-data to the destination page.  Then update
        // the count of tuples in the destination page.

        // Make room for the data
        rightSibling.dbPage.moveDataRange(OFFSET_FIRST_TUPLE,
            OFFSET_FIRST_TUPLE + len,
            rightSibling.endOffset - OFFSET_FIRST_TUPLE);

        // Copy the tuple-data across
        rightSibling.dbPage.write(OFFSET_FIRST_TUPLE, dbPage.getPageData(),
            startOffset, len);

        // Update the tuple-count
        rightSibling.dbPage.writeShort(OFFSET_NUM_TUPLES,
            rightSibling.numTuples + count);

        // Remove that range of tuple-data from this page.
        dbPage.writeShort(OFFSET_NUM_TUPLES, numTuples - count);

        // Only erase the old data in the leaf page if we are trying to make
        // sure everything works properly.
        if (BTreeTupleFile.CLEAR_OLD_DATA)
            dbPage.setDataRange(startOffset, len, (byte) 0);

        // Update the cached info for both leaves.
        loadPageContents();
        rightSibling.loadPageContents();
    }
}
