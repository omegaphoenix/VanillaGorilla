package edu.caltech.nanodb.storage;


/**
 * <p>
 * This interface provides the basic "pin" and "unpin" operations that
 * pinnable objects need to provide.  An object's pin-count is simply a
 * reference count, but with a shorter name so it's easier to type!
 * </p>
 * <p>
 * Currently, tuples and data pages are pinnable.
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
