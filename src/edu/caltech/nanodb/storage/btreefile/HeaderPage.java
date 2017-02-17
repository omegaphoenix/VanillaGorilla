package edu.caltech.nanodb.storage.btreefile;


import edu.caltech.nanodb.storage.DBFileType;
import edu.caltech.nanodb.storage.DBPage;
import org.apache.log4j.Logger;


/**
 * This class manipulates the header page for a B<sup>+</sup> tree index file.
 * The header page has the following structure:
 *
 * <ul>
 *   <li><u>Byte 0:</u>  {@link DBFileType#BTREE_TUPLE_FILE} (unsigned byte)</li>
 *   <li><u>Byte 1:</u>  page size  <i>p</i> (unsigned byte) - file's page
 *       size is <i>P</i> = 2<sup>p</sup></li>
 *
 *   <li>Byte 2-M:  Specification of index key-columns and column ordering.</li>
 *   <li>Byte P-2 to P-1:  the page of the file that is the root of the index</li>
 * </ul>
 */
public class HeaderPage {
    /** A logging object for reporting anything interesting that happens. */
    private static Logger logger = Logger.getLogger(HeaderPage.class);


    /**
     * The offset in the header page where the page number of the index's root
     * page is stored.  This value is an unsigned short.
     */
    public static final int OFFSET_ROOT_PAGE = 2;


    /**
     * The offset in the header page where the page number of the first leaf
     * page of the file is stored.  This allows the leaves of the tuple file
     * to be iterated through in sequential order.  This value is an unsigned
     * short.
     */
    public static final int OFFSET_FIRST_LEAF_PAGE = 4;


    /**
     * The offset in the header page where the page number of the first empty
     * page in the free list is stored.  This value is an unsigned short.
     */
    public static final int OFFSET_FIRST_EMPTY_PAGE = 6;


    /**
     * The offset in the header page where the length of the file's schema is
     * stored.  The statistics follow immediately after the schema.
     */
    public static final int OFFSET_SCHEMA_SIZE = 8;


    /**
     * The offset in the header page where the size of the table statistics
     * are stored.  This value is an unsigned short.
     */
    public static final int OFFSET_STATS_SIZE = 10;


    /**
     * The offset in the header page where the table schema starts.  This
     * value is an unsigned short.
     */
    public static final int OFFSET_SCHEMA_START = 12;


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
     * Returns the page-number of the root page in the index file.
     *
     * @param dbPage the header page of the index file
     * @return the page-number of the root page, or 0 if the index file doesn't
     *         contain a root page.
     */
    public static int getRootPageNo(DBPage dbPage) {
        verifyIsHeaderPage(dbPage);
        return dbPage.readUnsignedShort(OFFSET_ROOT_PAGE);
    }


    /**
     * Sets the page-number of the root page in the header page of the index
     * file.
     *
     * @param dbPage the header page of the heap table file
     * @param rootPageNo the page-number of the root page, or 0 if the index
     *        file doesn't contain a root page.
     */
    public static void setRootPageNo(DBPage dbPage, int rootPageNo) {
        verifyIsHeaderPage(dbPage);

        if (rootPageNo < 0) {
            throw new IllegalArgumentException(
                "rootPageNo must be > 0; got " + rootPageNo);
        }

        dbPage.writeShort(OFFSET_ROOT_PAGE, rootPageNo);
    }


    /**
     * Returns the page-number of the first leaf page in the index file.
     *
     * @param dbPage the header page of the index file
     * @return the page-number of the first leaf page in the index file, or 0
     *         if the index file doesn't contain any leaf pages.
     */
    public static int getFirstLeafPageNo(DBPage dbPage) {
        verifyIsHeaderPage(dbPage);
        return dbPage.readUnsignedShort(OFFSET_FIRST_LEAF_PAGE);
    }


    /**
     * Sets the page-number of the first leaf page in the header page of the
     * index file.
     *
     * @param dbPage the header page of the heap table file
     * @param firstLeafPageNo the page-number of the first leaf page in the
     *        index file, or 0 if the index file doesn't contain any leaf pages.
     */
    public static void setFirstLeafPageNo(DBPage dbPage, int firstLeafPageNo) {
        verifyIsHeaderPage(dbPage);

        if (firstLeafPageNo < 0) {
            throw new IllegalArgumentException(
                "firstLeafPageNo must be >= 0; got " + firstLeafPageNo);
        }

        dbPage.writeShort(OFFSET_FIRST_LEAF_PAGE, firstLeafPageNo);
    }


    /**
     * Returns the page-number of the first empty page in the index file.
     * Empty pages form a linked chain in the index file, so that they are
     * easy to locate.
     *
     * @param dbPage the header page of the index file
     * @return the page-number of the last leaf page in the index file.
     */
    public static int getFirstEmptyPageNo(DBPage dbPage) {
        verifyIsHeaderPage(dbPage);
        return dbPage.readUnsignedShort(OFFSET_FIRST_EMPTY_PAGE);
    }


    /**
     * Sets the page-number of the first empty page in the header page of the
     * index file.  Empty pages form a linked chain in the index file, so that
     * they are easy to locate.
     *
     * @param dbPage the header page of the heap table file
     * @param firstEmptyPageNo the page-number of the first empty page
     */
    public static void setFirstEmptyPageNo(DBPage dbPage, int firstEmptyPageNo) {
        verifyIsHeaderPage(dbPage);

        if (firstEmptyPageNo < 0) {
            throw new IllegalArgumentException(
                "firstEmptyPageNo must be >= 0; got " + firstEmptyPageNo);
        }

        dbPage.writeShort(OFFSET_FIRST_EMPTY_PAGE, firstEmptyPageNo);
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
