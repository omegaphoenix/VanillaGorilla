package edu.caltech.nanodb.storage.heapfile;


import edu.caltech.nanodb.relations.Schema;
import edu.caltech.nanodb.relations.Tuple;

import edu.caltech.nanodb.storage.DBPage;
import edu.caltech.nanodb.storage.FilePointer;
import edu.caltech.nanodb.storage.IndexedTableManager;
import edu.caltech.nanodb.storage.PageTuple;


/**
 */
public class HeapFilePageTuple extends PageTuple {
    /**
     * The slot that this tuple corresponds to.  The tuple doesn't actually
     * manipulate the slot table directly; that is for the
     * {@link IndexedTableManager} to deal with.
     */
    private int slot;


    /**
     * Construct a new tuple object that is backed by the data in the database
     * page.  This tuple is able to be read from or written to.
     *
     * @param schema the schema of the tuple file the page is a part of
     *
     * @param dbPage the specific database page that holds the tuple
     *
     * @param slot the slot number of the tuple
     *
     * @param pageOffset the offset of the tuple's actual data in the page
     */
    public HeapFilePageTuple(Schema schema, DBPage dbPage, int slot,
                             int pageOffset) {
        super(dbPage, pageOffset, schema);

        if (slot < 0) {
            throw new IllegalArgumentException(
                "slot must be nonnegative; got " + slot);
        }

        if (DataPage.getSlotValue(dbPage, slot) != pageOffset) {
            throw new IllegalArgumentException(String.format(
                "Offset %d in slot %d doesn't match pageOffset value %d",
                DataPage.getSlotValue(dbPage, slot), slot, pageOffset));
        }

        this.slot = slot;
    }


    /**
     * This method returns an external reference to the tuple, which references
     * the page number and slot-offset of the tuple.
     *
     * @return a file-pointer that can be used to look up this tuple
     */
    public FilePointer getExternalReference() {
        return new FilePointer(getDBPage().getPageNo(),
                               DataPage.getSlotOffset(slot));
    }


    protected void insertTupleDataRange(int off, int len) {
        DataPage.insertTupleDataRange(this.getDBPage(), off, len);
    }


    protected void deleteTupleDataRange(int off, int len) {
        DataPage.deleteTupleDataRange(this.getDBPage(), off, len);
    }


    public int getSlot() {
        return slot;
    }


    public static HeapFilePageTuple storeNewTuple(Schema schema,
        DBPage dbPage, int slot, int pageOffset, Tuple tuple) {

        PageTuple.storeTuple(dbPage, pageOffset, schema, tuple);

        return new HeapFilePageTuple(schema, dbPage, slot, pageOffset);
    }
}
