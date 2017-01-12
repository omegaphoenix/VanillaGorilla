package edu.caltech.nanodb.server;


import edu.caltech.nanodb.relations.TableInfo;
import edu.caltech.nanodb.relations.Tuple;

/**
 * <p>
 * This interface can be implemented by components that need to do processing
 * before and/or after a row is inserted, updated, or deleted.  Row-event
 * listeners need to be registered on the {@link EventDispatcher} before they
 * will be invoked.
 * </p>
 * <p>
 * Note that a specific naming convention is followed in the arguments for this
 * interface.  All "old" and "new" values are specified as {@link Tuple}
 * objects, but whether a tuple is actually in the referenced table depends on
 * the operation being performed.  For example, in the
 * {@link #beforeRowInserted} method, the "new tuple" isn't actually in the
 * table yet, because the insertion hasn't yet taken place.  Similarly, in the
 * {@link #afterRowDeleted} method, the "old tuple" is already removed from the
 * table by the time the method is invoked.
 * </p>
 * <p>
 * Therefore, the naming convention in this file is that arguments named
 * {@code oldValues} or {@code newValues} are not in the referenced table, but
 * {@code oldTuple} or {@code newTuple} are in the referenced table.
 * </p>
 * 
 * @design (donnie) We have separate insert/update/delete methods on the
 *         listener interface, because when these events are fired we know
 *         exactly what method to invoke, and there isn't any point in
 *         introducing the need to dispatch based on the operation being
 *         performed.
 */
public interface RowEventListener {
    /**
     * Perform processing before a row is inserted into a table.
     *
     * @param tblFileInfo the table that the tuple will be inserted into.
     *
     * @param newValues the new values that will be inserted into the table.
     */
    void beforeRowInserted(TableInfo tblFileInfo, Tuple newValues)
        throws Exception;


    /**
     * Perform processing after a row is inserted into a table.
     *
     * @param tblFileInfo the table that the tuple was inserted into.
     *
     * @param newTuple the new tuple that was inserted into the table.
     */
    void afterRowInserted(TableInfo tblFileInfo, Tuple newTuple)
        throws Exception;


    /**
     * Perform processing before a row is updated in a table.
     *
     * @param tblFileInfo the table that the tuple will be updated in.
     *
     * @param oldTuple the old tuple in the table that is about to be updated.
     *
     * @param newValues the new values that the tuple will be updated to.
     */
    void beforeRowUpdated(TableInfo tblFileInfo, Tuple oldTuple,
                          Tuple newValues) throws Exception;


    /**
     * Perform processing after a row is updated in a table.
     *
     * @param tblFileInfo the table that the tuple was updated in.
     *
     * @param oldValues the old values that were in the tuple before it was
     *                  updated.
     *
     * @param newTuple the new tuple in the table that was updated.
     */
    void afterRowUpdated(TableInfo tblFileInfo, Tuple oldValues,
                         Tuple newTuple) throws Exception;


    /**
     * Perform processing after a row has been deleted from a table.
     *
     * @param tblFileInfo the table that the tuple will be deleted from.
     *
     * @param oldTuple the old tuple in the table that is about to be deleted.
     */
    void beforeRowDeleted(TableInfo tblFileInfo, Tuple oldTuple)
        throws Exception;


    /**
     * Perform processing after a row has been deleted from a table.
     *
     * @param tblFileInfo the table that the tuple was deleted from.
     *
     * @param oldValues the old values that were in the tuple before it was
     *                  deleted.
     */
    void afterRowDeleted(TableInfo tblFileInfo, Tuple oldValues)
        throws Exception;
}
