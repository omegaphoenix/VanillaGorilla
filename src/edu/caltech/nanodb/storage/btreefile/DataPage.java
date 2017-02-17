package edu.caltech.nanodb.storage.btreefile;


/**
 * Created with IntelliJ IDEA.
 * User: donnie
 * Date: 2/4/13
 * Time: 6:51 PM
 * To change this template use File | Settings | File Templates.
 */
public interface DataPage {
    /** The page type always occupies the first byte of the page. */
    public static final int OFFSET_PAGE_TYPE = 0;
}
