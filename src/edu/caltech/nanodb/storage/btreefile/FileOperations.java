package edu.caltech.nanodb.storage.btreefile;

import java.io.IOException;

import edu.caltech.nanodb.storage.DBFile;
import edu.caltech.nanodb.storage.DBPage;
import edu.caltech.nanodb.storage.StorageManager;
import org.apache.log4j.Logger;


/**
 * Created by donnie on 2/26/14.
 */
class FileOperations {
    /** A logging object for reporting anything interesting that happens. */
    private static Logger logger = Logger.getLogger(FileOperations.class);


    private StorageManager storageManager;

    private DBFile dbFile;


    public FileOperations(StorageManager storageManager, DBFile dbFile) {
        this.storageManager = storageManager;
        this.dbFile = dbFile;
    }


    /**
     * This helper function finds and returns a new data page, either by
     * taking it from the empty-pages list in the file, or if the list is
     * empty, creating a brand new page at the end of the file.
     *
     * @return an empty {@code DBPage} that can be used for storing tuple data
     *
     * @throws IOException if an error occurs while loading a data page, or
     *         while extending the size of the B<sup>+</sup> tree file.
     */
    public DBPage getNewDataPage() throws IOException {
        DBPage dbpHeader = storageManager.loadDBPage(dbFile, 0);

        DBPage newPage;
        int pageNo = HeaderPage.getFirstEmptyPageNo(dbpHeader);

        if (pageNo == 0) {
            // There are no empty pages.  Create a new page to use.

            logger.debug("No empty pages.  Extending BTree file " + dbFile +
                             " by one page.");

            int numPages = dbFile.getNumPages();
            newPage = storageManager.loadDBPage(dbFile, numPages, true);
        }
        else {
            // Load the empty page, and remove it from the chain of empty pages.

            logger.debug("First empty page number is " + pageNo);

            newPage = storageManager.loadDBPage(dbFile, pageNo);
            int nextEmptyPage = newPage.readUnsignedShort(1);
            HeaderPage.setFirstEmptyPageNo(dbpHeader, nextEmptyPage);
        }

        logger.debug("Found data page to use:  page " + newPage.getPageNo());

        // TODO:  Increment the number of data pages?

        return newPage;
    }


    /**
     * This helper function marks a data page in the B<sup>+</sup> tree file
     * as "empty", and adds it to the list of empty pages in the file.
     *
     * @param dbPage the data-page that is no longer used.
     *
     * @throws IOException if an IO error occurs while releasing the data page,
     *         such as not being able to load the header page.
     */
    public void releaseDataPage(DBPage dbPage) throws IOException {
        // TODO:  If this page is the last page of the index file, we could
        //        truncate pages off the end until we hit a non-empty page.
        //        Instead, we'll leave all the pages around forever...

        DBFile dbFile = dbPage.getDBFile();

        // Record in the page that it is empty.
        dbPage.writeByte(0, BTreePageTypes.BTREE_EMPTY_PAGE);

        DBPage dbpHeader = storageManager.loadDBPage(dbFile, 0);

        // Retrieve the old "first empty page" value, and store it in this page.
        int prevEmptyPageNo = HeaderPage.getFirstEmptyPageNo(dbpHeader);
        dbPage.writeShort(1, prevEmptyPageNo);

        if (BTreeTupleFile.CLEAR_OLD_DATA) {
            // Clear out the remainder of the data-page since it's now unused.
            dbPage.setDataRange(3, dbPage.getPageSize() - 3, (byte) 0);
        }

        // Store the new "first empty page" value into the header.
        HeaderPage.setFirstEmptyPageNo(dbpHeader, dbPage.getPageNo());
    }
}
