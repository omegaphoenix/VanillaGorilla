package edu.caltech.nanodb.expressions;


/**
 * This class provides a type to associate with <tt>NULL</tt> literal values.
 * It really shouldn't be used in any other context.
 */
public class Null {
    private Null() {
        throw new UnsupportedOperationException(
            "This class should not be instantiated");
    }
}
