package edu.caltech.nanodb.storage.heapfile;


import org.apache.log4j.Logger;

import edu.caltech.nanodb.storage.DBPage;


/**
 * <p>
 * This class contains constants and basic functionality for accessing and
 * manipulating the contents of the header page of a heap table-file.  <b>Note
 * that the first two bytes of the first page is always devoted to the type and
 * page-size of the data file.</b>  (See {@link edu.caltech.nanodb.storage.DBFile}
 * for details.)  All other values must follow the first two bytes.
 * </p>
 * <p>
 * Heap table-file header pages are laid out as follows:
 * </p>
 * <ul>
 *   <li>As with all <tt>DBFile</tt>s, the first two bytes are the file type
 *       and page size, as always.</li>
 *   <li>After this come several values specifying the sizes of various areas in
 *       the header page, including the size of the table's schema specification,
 *       the statistics for the table, and the number of columns.</li>
 *   <li>Next the table's schema is recorded in the header page.  See the
 *       {@link edu.caltech.nanodb.storage.SchemaWriter} class for details on
 *       how a table's schema is stored.</li>
 *   <li>Finally, the table's statistics are stored.  See the
 *       {@link edu.caltech.nanodb.storage.StatsWriter} class for details on
 *       how a table's statistics are stored.</li>
 * </ul>
 * <p>
 * Even with all this information, usually only a few hundred bytes are required
 * for storing the details of most tables.
 * </p>
 *
 * @design (Donnie) Why is this class a static class, instead of a wrapper class
 *         around the {@link DBPage}?  No particular reason, really.  The class
 *         is used relatively briefly when a table is being accessed, and there
 *         is no real need for it to manage its own object-state, so it was just
 *         as convenient to provide all functionality as static methods.  This
 *         avoids the (small) overhead of instantiating an object as well.  But
 *         really, these are not particularly strong reasons.
 */
public class HeaderPage {
    /** A logging object for reporting anything interesting that happens. */
    private static Logger logger = Logger.getLogger(HeaderPage.class);


    /**
     * The offset in the header page where the size of the table schema is
     * stored.  This value is an unsigned short.
     */
    public static final int OFFSET_SCHEMA_SIZE = 2;


    /**
     * The offset in the header page where the size of the table statistics
     * are stored.  This value is an unsigned short.
     */
    public static final int OFFSET_STATS_SIZE = 4;


    /**
     * The offset in the header page where the table schema starts.  This
     * value is an unsigned short.
     */
    public static final int OFFSET_SCHEMA_START = 6;


    /**
     * This helper method simply verifies that the data page provided to the
     * <tt>HeaderPage</tt> class is in fact a header-page (i.e. page 0 in the
     * data file).
     *
     * @param dbPage the page to check
     *
     * @throws IllegalArgumentException if <tt>dbPage</tt> is <tt>null</tt>, or
     *         if it's not actually page 0 in the table file
     */
    private static void verifyIsHeaderPage(DBPage dbPage) {
        if (dbPage == null)
            throw new IllegalArgumentException("dbPage cannot be null");

        if (dbPage.getPageNo() != 0) {
            throw new IllegalArgumentException(
                "Page 0 is the header page in this storage format; was given page " +
                dbPage.getPageNo());
        }
    }


    /**
     * Returns the number of bytes that the table's schema occupies for storage
     * in the header page.
     *
     * @param dbPage the header page of the heap table file
     * @return the number of bytes that the table's schema occupies
     */
    public static int getSchemaSize(DBPage dbPage) {
        verifyIsHeaderPage(dbPage);
        return dbPage.readUnsignedShort(OFFSET_SCHEMA_SIZE);
    }


    /**
     * Sets the number of bytes that the table's schema occupies for storage
     * in the header page.
     *
     * @param dbPage the header page of the heap table file
     * @param numBytes the number of bytes that the table's schema occupies
     */
    public static void setSchemaSize(DBPage dbPage, int numBytes) {
        verifyIsHeaderPage(dbPage);

        if (numBytes < 0) {
            throw new IllegalArgumentException(
                "numButes must be >= 0; got " + numBytes);
        }

        dbPage.writeShort(OFFSET_SCHEMA_SIZE, numBytes);
    }


    /**
     * Returns the number of bytes that the table's statistics occupy for
     * storage in the header page.
     *
     * @param dbPage the header page of the heap table file
     * @return the number of bytes that the table's statistics occupy
     */
    public static int getStatsSize(DBPage dbPage) {
        verifyIsHeaderPage(dbPage);
        return dbPage.readUnsignedShort(OFFSET_STATS_SIZE);
    }


    /**
     * Sets the number of bytes that the table's statistics occupy for storage
     * in the header page.
     *
     * @param dbPage the header page of the heap table file
     * @param numBytes the number of bytes that the table's statistics occupy
     */
    public static void setStatsSize(DBPage dbPage, int numBytes) {
        verifyIsHeaderPage(dbPage);

        if (numBytes < 0) {
            throw new IllegalArgumentException(
                "numButes must be >= 0; got " + numBytes);
        }

        dbPage.writeShort(OFFSET_STATS_SIZE, numBytes);
    }


    /**
     * Returns the offset in the header page that the table statistics start at.
     * This value changes because the table schema resides before the stats, and
     * therefore the stats don't live at a fixed location.
     *
     * @param dbPage the header page of the heap table file
     * @return the offset within the header page that the table statistics
     *         reside at
     */
    public static int getStatsOffset(DBPage dbPage) {
        verifyIsHeaderPage(dbPage);
        return OFFSET_SCHEMA_START + getSchemaSize(dbPage);
    }
}
