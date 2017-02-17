package edu.caltech.nanodb.storage.btreefile;


/**
 * This interface specifies the page-type values that may appear within the
 * B<sup>+</sup> Tree implementation.
 *
 * @design (donnie) We use this instead of an {@code enum} since the values
 *         are actually read and written against pages in the B<sup>+</sup>
 *         Tree file.  Note that there is no page-type value for the root
 *         page; that page is considered separately.
 *
 * @design (donnie) This class is package-private since it is an internal
 *         implementation detail and we want to keep it local to the
 *         {@code btreefile} package.
 */
final class BTreePageTypes {
    /**
     * This value is stored in a B-tree page's byte 0, to indicate that the
     * page is an inner (i.e. non-leaf) page.
     */
    public static final int BTREE_INNER_PAGE = 1;


    /**
     * This value is stored in a B-tree page's byte 0, to indicate that the
     * page is a leaf page.
     */
    public static final int BTREE_LEAF_PAGE = 2;


    /**
     * This value is stored in a B-tree page's byte 0, to indicate that the
     * page is empty.
     */
    public static final int BTREE_EMPTY_PAGE = 3;
}
