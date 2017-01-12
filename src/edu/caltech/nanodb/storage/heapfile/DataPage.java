package edu.caltech.nanodb.storage.heapfile;


import org.apache.log4j.Logger;

import edu.caltech.nanodb.storage.DBPage;


/**
 * This class provides the constants and operations necessary for manipulating
 * a data page within a heap file.  Heap files use a slotted-page structure
 * where tuple-data is filled from the end of the page forward, so for two slots
 * at indexes <em>i</em> and <em>j</em>, where <em>i</em> &lt; <em>j</em>, the
 * start of <em>i</em>'s tuple data will be <u>after</u> <em>j</em>'s tuple
 * data.
 *
 * @design (Donnie) Why is this class a static class, instead of a wrapper class
 *         around the {@link DBPage}?  No particular reason, really.  The class
 *         is used relatively briefly when a table is being accessed, and there
 *         is no real need for it to manage its own object-state, so it was just
 *         as convenient to provide all functionality as static methods.  This
 *         avoids the (small) overhead of instantiating an object as well.  But
 *         really, these are not particularly strong reasons.
 */
public class DataPage {
    /** A logging object for reporting anything interesting that happens. */
    private static Logger logger = Logger.getLogger(DataPage.class);


    /**
     * The offset in the data page where the number of slots in the slot table
     * is stored.
     */
    public static final int OFFSET_NUM_SLOTS = 0;


    /**
     * This offset-value is stored into a slot when it is empty.  It is set to
     * zero because this is where the page's slot-count is stored and therefore
     * this is obviously an invalid offset for a tuple to be located at.
     */
    public static final int EMPTY_SLOT = 0;


    /**
     * Initialize a newly allocated data page.  Currently this involves setting
     * the number of slots to 0.  There is no other internal structure in data
     * pages at this point.
     *
     * @param dbPage the data page to initialize
     */
    public static void initNewPage(DBPage dbPage) {
        setNumSlots(dbPage, 0);
    }


    public static int getSlotOffset(int slot) {
        return (1 + slot) * 2;
    }


    /**
     * Returns the number of slots in this data page.  This can be considered
     * to be the current "capacity" of the page, since any number of the slots
     * could be set to {@link #EMPTY_SLOT} to indicate that they are empty.
     *
     * @param dbPage the data page to retrieve the number of slots for
     * @return the current number of slots in the page
     */
    public static int getNumSlots(DBPage dbPage) {
        return dbPage.readUnsignedShort(OFFSET_NUM_SLOTS);
    }


    /**
     * Sets the number of slots in this data page.  Note that if the number of
     * slots is increased, the new slots <em>must</em> be initialized to
     * {@link #EMPTY_SLOT}, or else functions like {@link #getFreeSpaceInPage}
     * (which relies on the offset stored in the last slot) will produce wrong
     * results.
     *
     * @param dbPage the data page to set the number of slots for
     * @param numSlots the value to store
     */
    public static void setNumSlots(DBPage dbPage, int numSlots) {
        dbPage.writeShort(OFFSET_NUM_SLOTS, numSlots);
    }


    /**
     * This static helper function returns the index where the slot list ends in
     * the data page.
     *
     * @param dbPage the data page to examine
     * @return the index in the page data where the slot-table ends.  This also
     *         happens to be the size of the slot-table in bytes.
     */
    public static int getSlotsEndIndex(DBPage dbPage) {
        // Slots are at indexes [0, numSlots), so just pass the total number of
        // slots into the helper function.
        return getSlotOffset(getNumSlots(dbPage));
    }


    /**
     * This static helper function returns the value stored in the specified
     * slot.  This will either be the offset of the start of a tuple in the data
     * page, or it will be {@link #EMPTY_SLOT} if the slot is empty.
     *
     * @param dbPage the data page to retrieve the slot-value from
     * @param slot the slot to retrieve the value for; valid values are from 0
     *        to {@link #getNumSlots} - 1.
     *
     * @return the current value stored for the slot in the data page
     *
     * @throws IllegalArgumentException if the specified slot number is outside
     *         the range [0, {@link #getNumSlots}).
     */
    public static int getSlotValue(DBPage dbPage, int slot) {
        int numSlots = getNumSlots(dbPage);

        if (slot < 0 || slot >= numSlots) {
            throw new IllegalArgumentException("Valid slots are in range [0," +
                numSlots + ").  Got " + slot);
        }

        return dbPage.readUnsignedShort(getSlotOffset(slot));
    }


    /**
     * This static helper function sets the value for the specified slot.  This
     * should either be the offset of the start of a tuple in the data
     * page, or it should be {@link #EMPTY_SLOT} if the slot is empty.
     *
     * @param dbPage the data page to set the slot-value for
     * @param slot the slot to set the value of; valid values are from 0 to
     *        {@link #getNumSlots} - 1.
     * @param value the value to store for the slot
     *
     * @throws IllegalArgumentException if the specified slot number is outside
     *         the range [0, {@link #getNumSlots}).
     */
    public static void setSlotValue(DBPage dbPage, int slot, int value) {
        int numSlots = getNumSlots(dbPage);

        if (slot < 0 || slot >= numSlots) {
            throw new IllegalArgumentException("Valid slots are in range [0," +
                numSlots + ").  Got " + slot);
        }

        dbPage.writeShort(getSlotOffset(slot), value);
    }


    public static int getSlotIndexFromOffset(DBPage dbPage, int offset)
        throws IllegalArgumentException {

        if (offset % 2 != 0) {
            throw new IllegalArgumentException(
                "Slots occur at even indexes (each slot is a short).");
        }

        int slot = (offset - 2) / 2;
        int numSlots = getNumSlots(dbPage);

        if (slot < 0 || slot >= numSlots) {
            throw new IllegalArgumentException("Valid slots are in range [0," +
                numSlots + ").  Got " + slot);
        }

        return slot;
    }


    /**
     * This static helper function returns the index of where tuple data
     * currently starts in the specified data page.  This method uses the last
     * slot in the slot-table to determine where the tuple data begins, since
     * the slots must be organized in decreasing order of offset-value.  (In
     * other words, we add tuple data from the back of the block towards the
     * front.)
     *
     * @see #setNumSlots
     *
     * @param dbPage the data page to examine
     *
     * @return the index where the tuple data starts in this data page
     */
    public static int getTupleDataStart(DBPage dbPage) {
        int numSlots = getNumSlots(dbPage);

        // If there are no tuples in this page, "data start" is the top of the
        // page data.
        int dataStart = getTupleDataEnd(dbPage);

        int slot = numSlots - 1;
        while (slot >= 0) {
            int slotValue = getSlotValue(dbPage, slot);
            if (slotValue != EMPTY_SLOT) {
                dataStart = slotValue;
                break;
            }

            --slot;
        }

        return dataStart;
    }


    /**
     * This static helper function returns the index of where tuple data
     * currently ends in the specified data page.  This value depends more on
     * the overall structure of the data page, and at present is simply the
     * page-size.
     *
     * @param dbPage the data page to examine
     *
     * @return the index where the tuple data ends in this data page
     */
    public static int getTupleDataEnd(DBPage dbPage) {
        return dbPage.getPageSize();
    }


    /**
     * Returns the length of the tuple stored at the specified slot.  It is
     * invalid to use this method on an empty slot.
     *
     * @param dbPage the data page being examined
     * @param slot the slot of the tuple to retrieve the length of
     * @return the length of the tuple's data stored in this slot
     *
     * @throws IllegalArgumentException if the specified slot is invalid, or if
     *         the specified slot has {@link #EMPTY_SLOT} for its value
     */
    public static int getTupleLength(DBPage dbPage, int slot) {
        int numSlots = getNumSlots(dbPage);

        if (slot < 0 || slot >= numSlots) {
            throw new IllegalArgumentException(
                "Valid slots are in range [0," + slot + ").  Got " + slot);
        }

        int tupleStart = getSlotValue(dbPage, slot);

        if (tupleStart == EMPTY_SLOT)
            throw new IllegalArgumentException("Slot " + slot + " is empty.");

        int tupleLength = -1;

        int prevSlot = slot - 1;
        while (prevSlot >= 0) {
            int prevTupleStart = getSlotValue(dbPage, prevSlot);
            if (prevTupleStart != EMPTY_SLOT) {

                // Earlier slots have higher offsets.  (Yes it's weird.)
                tupleLength = prevTupleStart - tupleStart;

                break;
            }

            prevSlot--;
        }

        if (prevSlot < 0) {
            // The specified slot held the last tuple in the page.
            tupleLength = getTupleDataEnd(dbPage) - tupleStart;
        }

        return tupleLength;
    }


    /**
     * This static helper function returns the amount of free space in
     * a tuple data page.  It simply uses other methods in this class to
     * perform the simple computation.
     *
     * @param dbPage the data page to examine
     * @return the amount of free space in the data page, in bytes
     */
    public static int getFreeSpaceInPage(DBPage dbPage) {
        return getTupleDataStart(dbPage) - getSlotsEndIndex(dbPage);
    }


    /**
     * This static helper method verifies that the specified data page has
     * proper structure and organization by performing various sanity checks.
     * Currently, the only sanity check it performs is to verify that
     * slot-offsets do indeed decrease monotonically as the slot-index
     * increases.
     * <p>
     * Any issues will cause warnings to be logged.
     *
     * @param dbPage the data page to check
     */
    public static void sanityCheck(DBPage dbPage) {
        int numSlots = getNumSlots(dbPage);
        if (numSlots == 0)
            return;

        // Find the first occupied slot, and get its offset into prevOffset.
        int iSlot = -1;
        int prevSlot = -1;
        int prevOffset = EMPTY_SLOT;
        while (iSlot + 1 < numSlots && prevOffset == EMPTY_SLOT) {
            iSlot++;
            prevSlot = iSlot;
            prevOffset = getSlotValue(dbPage, iSlot);
        }

        while (iSlot + 1 < numSlots) {

            // Find the next occupied slot, and get its offset into offset.
            int offset = EMPTY_SLOT;
            while (iSlot + 1 < numSlots && offset == EMPTY_SLOT) {
                iSlot++;
                offset = getSlotValue(dbPage, iSlot);
            }

            if (iSlot < numSlots) {
                // Tuple offsets should be strictly decreasing.
                if (prevOffset <= offset) {
                    logger.warn(String.format(
                        "Slot %d and %d offsets are not strictly decreasing " +
                        "(%d should be greater than %d)", prevSlot, iSlot,
                        prevOffset, offset));
                }

                prevSlot = iSlot;
                prevOffset = offset;
            }
        }
    }



    /**
     * <p>
     * This static helper function creates a space in the data page of the
     * specified size, sliding tuple data below the offset down to
     * create a gap.  Because tuples below the specified offset will actually
     * move, some of the slots in the page may also need to be modified.
     * </p>
     * <p>The new space is initialized to all zero values.</p>
     *
     * @param dbPage The table data-page to insert space into.
     *
     * @param off The offset in the page where the space will be added.
     *
     * @param len The number of bytes to insert.
     */
    public static void insertTupleDataRange(DBPage dbPage, int off, int len) {

        int tupDataStart = getTupleDataStart(dbPage);

        if (off < tupDataStart) {
            throw new IllegalArgumentException("Specified offset " + off +
                " is not actually in the tuple data portion of this page " +
                "(data starts at offset " + tupDataStart + ").");
        }

        if (len < 0)
            throw new IllegalArgumentException("Length must not be negative.");

        if (len > getFreeSpaceInPage(dbPage)) {
            throw new IllegalArgumentException("Specified length " + len +
                " is larger than amount of free space in this page (" +
                getFreeSpaceInPage(dbPage) + " bytes).");
        }

        // If off == tupDataStart then there's no need to move anything.
        if (off > tupDataStart) {
            // Move the data in the range [tupDataStart, off) to
            // [tupDataStart - len, off - len).  Thus there will be a gap in the
            // range [off - len, off) after the operation is completed.

            dbPage.moveDataRange(tupDataStart, tupDataStart - len,
                off - tupDataStart);
        }

        // Zero out the gap that was just created.
        int startOff = off - len;
        dbPage.setDataRange(startOff, len, (byte) 0);

        // Update affected slots; this includes all slots below the specified
        // offset.  The update is easy; slot values just move down by len bytes.

        int numSlots = getNumSlots(dbPage);
        for (int iSlot = 0; iSlot < numSlots; iSlot++) {

            int slotValue = getSlotValue(dbPage, iSlot);
            if (slotValue != EMPTY_SLOT && slotValue < off) {
                // Update this slot's offset.
                slotValue -= len;
                setSlotValue(dbPage, iSlot, slotValue);
            }
        }
    }


    /**
     * This static helper function removes a sequence of bytes from the current
     * tuple data in the page, sliding tuple data below the offset forward to
     * fill in the gap.  Because tuples below the specified offset will actually
     * move, some of the slots in the page may also need to be modified.
     *
     * @param dbPage The table data-page to insert space into.
     *
     * @param off The offset in the page where the space will be removed.
     *
     * @param len The number of bytes to remove.
     */
    public static void deleteTupleDataRange(DBPage dbPage, int off, int len) {
        int tupDataStart = getTupleDataStart(dbPage);

        logger.debug(String.format(
            "Deleting tuple data-range offset %d, length %d", off, len));

        if (off < tupDataStart) {
            throw new IllegalArgumentException("Specified offset " + off +
                " is not actually in the tuple data portion of this page " +
                "(data starts at offset " + tupDataStart + ").");
        }

        if (len < 0)
            throw new IllegalArgumentException("Length must not be negative.");

        if (getTupleDataEnd(dbPage) - off < len) {
            throw new IllegalArgumentException("Specified length " + len +
                " is larger than size of tuple data in this page (" +
                (getTupleDataEnd(dbPage) - off) + " bytes).");
        }

        // Move the data in the range [tupDataStart, off) to
        // [tupDataStart + len, off + len).

        logger.debug(String.format(
            "    Moving %d bytes of data from [%d, %d) to [%d, %d)",
            off - tupDataStart, tupDataStart, off,
            tupDataStart + len, off + len));

        dbPage.moveDataRange(tupDataStart, tupDataStart + len, off - tupDataStart);

        // Update affected slots; this includes all (non-empty) slots whose
        // offset is below the specified offset.  The update is easy; slot
        // values just move up by len bytes.

        int numSlots = getNumSlots(dbPage);
        for (int iSlot = 0; iSlot < numSlots; iSlot++) {

            int slotValue = getSlotValue(dbPage, iSlot);
            if (slotValue != EMPTY_SLOT && slotValue <= off) {
                // Update this slot's offset.
                slotValue += len;
                setSlotValue(dbPage, iSlot, slotValue);
            }
        }
    }


    /**
     * Update the data page so that it has space for a new tuple of the
     * specified size.  The new tuple is assigned a slot (whose index is
     * returned by this method), and the space for the tuple is initialized
     * to all zero values.
     *
     * @param dbPage The data page to store the new tuple in.
     *
     * @param len The length of the new tuple's data.
     *
     * @return The slot-index for the new tuple.  The offset to the start
     *         of the requested space is available via that slot.  (Use
     *         {@link #getSlotValue} to retrieve that offset.)
     */
    public static int allocNewTuple(DBPage dbPage, int len) {

        if (len < 0) {
            throw new IllegalArgumentException(
                "Length must be nonnegative; got " + len);
        }

        // The amount of free space we need in the database page, if we are
        // going to add the new tuple.
        int spaceNeeded = len;

        logger.debug("Allocating space for new " + len + "-byte tuple.");

        // Search through the current list of slots in the page.  If a slot
        // is marked as "empty" then we can use that slot.  Otherwise, we
        // will need to add a new slot to the end of the list.

        int slot;
        int numSlots = getNumSlots(dbPage);

        logger.debug("Current number of slots on page:  " + numSlots);

        // This variable tracks where the new tuple should END.  It starts
        // as the page-size, and gets moved down past each valid tuple in
        // the page, until we find an available slot in the page.
        int newTupleEnd = getTupleDataEnd(dbPage);

        for (slot = 0; slot < numSlots; slot++) {
            // currSlotValue is either the start of that slot's tuple-data,
            // or it is set to EMPTY_SLOT.
            int currSlotValue = getSlotValue(dbPage, slot);

            if (currSlotValue == EMPTY_SLOT)
                break;
            else
                newTupleEnd = currSlotValue;
        }

        // First make sure we actually have enough space for the new tuple.

        if (slot == numSlots) {
            // We'll need to add a new slot to the list.  Make sure there's
            // room.
            spaceNeeded += 2;
        }

        if (spaceNeeded > getFreeSpaceInPage(dbPage)) {
            // Switch this to a checked exception?  The table manager has
            // already verified that the page should have enough space, so if
            // this fails, it would indicate a bug.  So, runtime exception is
            // fine for now.
            throw new IllegalArgumentException(
                "Space needed for new tuple (" + spaceNeeded +
                " bytes) is larger than the free space in this page (" +
                getFreeSpaceInPage(dbPage) + " bytes).");
        }

        // Now we know we have space for the tuple.  Update the slot list,
        // and the update page's layout to make room for the new tuple.

        if (slot == numSlots) {
            logger.debug("No empty slot available.  Adding a new slot.");

            // Add the new slot to the page, and update the total number of
            // slots.
            numSlots++;
            setNumSlots(dbPage, numSlots);
            setSlotValue(dbPage, slot, EMPTY_SLOT);
        }

        logger.debug(String.format(
            "Tuple will get slot %d.  Final number of slots:  %d",
            slot, numSlots));

        int newTupleStart = newTupleEnd - len;

        logger.debug(String.format(
            "New tuple of %d bytes will reside at location [%d, %d).",
            len, newTupleStart, newTupleEnd));

        // Make room for the new tuple's data to be stored into.  Since
        // tuples are stored from the END of the page going backwards, we
        // specify the new tuple's END index, and the tuple's length.
        // (Note:  This call also updates all affected slots whose offsets
        // would be changed.)
        insertTupleDataRange(dbPage, newTupleEnd, len);

        // Set the slot's value to be the starting offset of the tuple.
        // We have to do this *after* we insert the new space for the new
        // tuple, or else insertTupleDataRange() will clobber the
        // slot-value of this tuple.
        setSlotValue(dbPage, slot, newTupleStart);

        // Finally, return the slot-index of the new tuple.
        return slot;
    }


    /**
     * Deletes the tuple at the specified slot from the data page.  The space
     * occupied by the tuple's data is reclaimed by sliding tuple-data lower
     * in the page upward.  Also, any trailing slots that are now marked as
     * "deleted" are reclaimed.
     *
     * @param dbPage the data page to remove the tuple from
     * @param slot the slot of the tuple to delete
     */
    public static void deleteTuple(DBPage dbPage, int slot) {

        if (slot < 0) {
            throw new IllegalArgumentException(
                "Slot must be nonnegative; got " + slot);
        }

        int numSlots = getNumSlots(dbPage);

        if (slot >= numSlots) {
            throw new IllegalArgumentException("Page only has " + numSlots +
                " slots, but slot " + slot + " was requested for deletion.");
        }

        // TODO:  Complete this implementation.
        throw new UnsupportedOperationException("TODO:  Implement!");
    }
}
