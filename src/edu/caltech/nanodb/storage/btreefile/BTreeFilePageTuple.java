package edu.caltech.nanodb.storage.btreefile;


import edu.caltech.nanodb.relations.Schema;
import edu.caltech.nanodb.storage.DBPage;
import edu.caltech.nanodb.storage.PageTuple;


/**
 * <p>
 * This class uses the <tt>PageTuple</tt> class functionality to access and
 * manipulate keys stored in a B<sup>+</sup> tree tuple file.  There is one
 * extension, which is to allow the tuple to remember its index within the
 * leaf page it is from; this makes it easy to move to the next tuple within
 * the page very easily.
 * </p>
 * <p>
 * B<sup>+</sup> tree tuple deletion is interesting, since all the tuples form
 * a linear sequence in the page.  When a given tuple T is deleted, the next
 * tuple ends up at the same position that T was at.  Therefore, to implement
 * the tuple file's "get next tuple" functionality properly, we must keep
 * track of whether the previous tuple was deleted or not; if it was deleted,
 * we don't advance in the page.
 * </p>
 */
public class BTreeFilePageTuple extends PageTuple {

    private int tupleIndex;


    /**
     * Records if this tuple has been deleted or not.  This affects navigation
     * to the next tuple in the current page, since removal of the current
     * tuple causes this object to point to the next tuple.
     */
    private boolean deleted = false;

    /**
     * If this tuple is deleted, this field will be set to the page number of
     * the next tuple.  If there are no more tuples then this will be set to
     * -1.
     */
    private int nextTuplePageNo;

    /**
     * If this tuple is deleted, this field will be set to the index of the
     * next tuple.  If there are no more tuples then this will be set to -1.
     */
    private int nextTupleIndex;


    public BTreeFilePageTuple(Schema schema, DBPage dbPage, int pageOffset,
                              int tupleIndex) {
        super(dbPage, pageOffset, schema);

        if (tupleIndex < 0) {
            throw new IllegalArgumentException(
                "tupleIndex must be at least 0, got " + tupleIndex);
        }

        this.tupleIndex = tupleIndex;
    }


    public int getTupleIndex() {
        return tupleIndex;
    }


    public boolean isDeleted() {
        return deleted;
    }


    public void setDeleted() {
        deleted = true;
    }


    public void setNextTuplePosition(int pageNo, int tupleIndex) {
        if (!deleted)
            throw new IllegalStateException("Tuple must be deleted");

        nextTuplePageNo = pageNo;
        nextTupleIndex = tupleIndex;
    }


    public int getNextTuplePageNo() {
        if (!deleted)
            throw new IllegalStateException("Tuple must be deleted");

        return nextTuplePageNo;
    }


    public int getNextTupleIndex() {
        if (!deleted)
            throw new IllegalStateException("Tuple must be deleted");

        return nextTupleIndex;
    }


    @Override
    protected void insertTupleDataRange(int off, int len) {
        throw new UnsupportedOperationException(
            "B+ Tree index tuples don't support resizing.");
    }


    @Override
    protected void deleteTupleDataRange(int off, int len) {
        throw new UnsupportedOperationException(
            "B+ Tree index tuples don't support resizing.");
    }


    @Override
    public String toString() {
        StringBuilder buf = new StringBuilder();
        buf.append("BTPT[");

        if (deleted) {
            buf.append("deleted");
        }
        else {
            boolean first = true;
            for (int i = 0; i < getColumnCount(); i++) {
                if (first)
                    first = false;
                else
                    buf.append(',');

                Object obj = getColumnValue(i);
                if (obj == null)
                    buf.append("NULL");
                else
                    buf.append(obj);
            }
        }

        buf.append(']');

        return buf.toString();
    }
}
