package edu.caltech.nanodb.util;


/**
 * Some helpful utility operations for working with arrays.
 */
public class ArrayUtil {
    /**
     * This function reports how many bytes are identical between two arrays,
     * starting at the specified index.  The arrays are expected to be the same
     * length.
     *
     * @param a the first array to examine
     *
     * @param b the second array to examine
     *
     * @param index the index to start the comparison at
     *
     * @return the number of bytes that are the same, starting from the
     *         specified index
     */
    public static int sizeOfIdenticalRange(byte[] a, byte[] b, int index) {
        if (a == null)
            throw new IllegalArgumentException("a must be specified");

        if (b == null)
            throw new IllegalArgumentException("b must be specified");

        if (a.length != b.length)
            throw new IllegalArgumentException("a and b must be the same size");

        if (index < 0 || index >= a.length) {
            throw new IllegalArgumentException(
                "off must be a valid index into the arrays");
        }

        int size = 0;
        for (int i = index; i < a.length && a[i] == b[i]; i++, size++);

        return size;
    }


    /**
     * This function reports how many bytes are different between two arrays,
     * starting at the specified index.  The arrays are expected to be the same
     * length.
     *
     * @param a the first array to examine
     *
     * @param b the second array to examine
     *
     * @param index the index to start the comparison at
     *
     * @return the number of bytes that are different, starting from the
     *         specified index
     */
    public static int sizeOfDifferentRange(byte[] a, byte[] b, int index) {
        if (a == null)
            throw new IllegalArgumentException("a must be specified");

        if (b == null)
            throw new IllegalArgumentException("b must be specified");

        if (a.length != b.length)
            throw new IllegalArgumentException("a and b must be the same size");

        if (index < 0 || index >= a.length) {
            throw new IllegalArgumentException(
                "off must be a valid index into the arrays");
        }

        int size = 0;
        for (int i = index; i < a.length && a[i] != b[i]; i++, size++);

        return size;
    }
}
