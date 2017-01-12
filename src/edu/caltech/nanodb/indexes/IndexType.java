package edu.caltech.nanodb.indexes;


/**
 * This enumeration represents the different kinds of indexes that NanoDB
 * supports, since different kinds of indexes facilitate different kinds of
 * lookups with different performance characteristics.
 */
public enum IndexType {
    /** Represents indexes that order values to determine a tuple's location. */
    ORDERED_INDEX,

    /** Represents indexes that hash values to determine a tuple's location. */
    HASHED_INDEX
}
