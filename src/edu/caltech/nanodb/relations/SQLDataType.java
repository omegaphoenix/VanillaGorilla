package edu.caltech.nanodb.relations;


/**
 * An enumeration of the SQL data-types that are supported by NanoDB.  NanoDB
 * does not support a dynamic or user-extensible type system, although the
 * process for programmatically adding new data-types should be relatively
 * straightforward.
 * <p>
 * Each data-type also has an integer type-ID associated with it, accessible via
 * the {@link #getTypeID} method.  The values start at 1 and go up.  Note that
 * type-IDs are currently <code>byte</code> values.  This is to facilitate
 * compact storage in the data dictionary.  If more than 127 data-types are
 * needed (which is unlikely), keep in mind that this will affect the storage
 * format in the data-dictionary.
 * <p>
 * The type-ID values currently fall in ranges so that similar types can be
 * grouped together.  Besides aesthetics, this also allows us to easily tell if
 * a type is a number, or a string, etc.  Here is the current breakdown:
 * <ul>
 *   <li>Number types:  1..20</li>
 *   <li>Character types:  21..30</li>
 *   <li>Date/time types:  31..40</li>
 *   <li>Miscellaneous types:  41..*</li>
 * </ul>
 */
public enum SQLDataType {

    /** A placeholder type for <tt>NULL</tt> literals */
    NULL((byte) 0),

    // Number data-types:

    /**
     * A 4-byte signed integer, designated in SQL as <tt>INTEGER</tt>.  It
     * corresponds to the Java <code>int</code> data-type.
     */
    INTEGER((byte) 1),

    /**
     * A 2-byte signed integer, designated in SQL as <tt>SMALLINT</tt>.  It
     * corresponds to the Java <code>short</code> data-type.
     */
    SMALLINT((byte) 2),

    /**
     * An 8-byte signed integer, designated in SQL as <tt>BIGINT</tt>.  It
     * corresponds to the Java <code>long</code> data-type.
     * This is not a standard SQL data-type.
     */
    BIGINT((byte) 3),

    /**
     * A 1-byte signed integer, designated in SQL as <tt>TINYINT</tt>.  It
     * corresponds to the Java <code>byte</code> data-type.
     * This is not a standard SQL data-type.
     */
    TINYINT((byte) 4),

    /**
     * A 4-byte signed floating-point number with 24 bits of precision, designated
     * in SQL as <tt>FLOAT</tt>.  It corresponds to the Java <code>float</code>
     * data-type.
     * <p>
     * The SQL standard contains this data-type, but specifies additional options
     * provided here.
     */
    FLOAT((byte) 5),

    /**
     * An 8-byte signed floating-point number with 53 bits of precision,
     * designated in SQL as <tt>DOUBLE</tt>.  It corresponds to the Java
     * <code>double</code> data-type.
     * <p>
     * The SQL standard contains this data-type, but specifies additional options
     * provided here.
     */
    DOUBLE((byte) 6),

    /** A decimal value with a specified precision and scale. */
    NUMERIC((byte) 7),

    // Character-sequence data-types:

    /** A fixed-length character-sequence, with a specified length. */
    CHAR((byte) 21),

    /** A variable-length character-sequence, with a specified maximum length. */
    VARCHAR((byte) 22),

    /** A large character-sequence, with a very large maximum length. */
    TEXT((byte) 23),

    /** A large byte-sequence, with a very large maximum length. */
    BLOB((byte) 24),

    // Date and time data-types:

    /** A date value containing year, month, and day. */
    DATE((byte) 31),

    /** A time value containing hours, minutes, and seconds. */
    TIME((byte) 32),

    /**
     * A combination date and time value, containing all the fields of
     * {@link #DATE} and {@link #TIME}.
     */
    DATETIME((byte) 33),

    /**
     * A date/time value with higher precision than the {@link #DATETIME} value.
     */
    TIMESTAMP((byte) 34),


    /**
     * A file-pointer value.  This is not exposed in SQL, but is used
     * internally.
     */
    FILE_POINTER((byte) 41);


    /** The ID of the datatype. */
    private final byte typeID;


    /**
     * Construct a new SQLDataType instance with the specified type ID.  Each
     * data-type should have a unique ID.  Note that this constructor is private,
     * since this is the only place where SQL data-types should be declared.
     *
     * @param typeID the unique numeric ID for this SQL data type
     */
    private SQLDataType(byte typeID) {
        this.typeID = typeID;
    }


    /**
     * Returns this SQL data-type's unique ID.
     *
     * @return this SQL data type's unique ID.
     */
    public byte getTypeID() {
        return typeID;
    }


    /**
     * Given the specified ID, this method returns the corresponding SQL data
     * type enum value, or it returns <code>null</code> if the type value
     * doesn't signify any SQL data type in this enumeration.
     *
     * @param typeID the ID of the type to search for
     * @return the <tt>SQLDataType</tt> value corresponding to the specified
     *         type ID, or <tt>null</tt> if no corresponding value could be
     *         found.
     */
    public static SQLDataType findType(byte typeID) {
        for (SQLDataType s : SQLDataType.values()) {
            if (s.getTypeID() == typeID)
                return s;
        }
        return null;
    }


    public static boolean isNumber(SQLDataType type) {
        return (type.typeID >= 1 && type.typeID <= 20);
    }


    public static boolean isString(SQLDataType type) {
        return (type.typeID >= 21 && type.typeID <= 23);
    }

}
