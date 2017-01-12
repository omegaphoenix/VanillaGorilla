package edu.caltech.nanodb.relations;


/** An enumeration specifying the constraint types allowed on tables. */
public enum TableConstraintType {

    /** Values in a column may not be null. */
    NOT_NULL(1),

    /** Values in a column or set of columns must be unique. */
    UNIQUE(2),

    /** The column or set of columns is the table's primary key. */
    PRIMARY_KEY(3),

    /** Values in the column or columns reference a key in another table. */
    FOREIGN_KEY(4);


    /** The ID of the constraint type. */
    private final byte typeID;


    /**
     * Construct a new TableConstraintType instance with the specified type ID.
     * Each constraint type should have a unique ID.  Note that this constructor
     * is private, since this is the only place where table constraint types
     * should be declared.
     */
    private TableConstraintType(int typeID) {
        this.typeID = (byte) typeID;
    }


    /** Returns the constraint type's unique ID. */
    public byte getTypeID() {
        return typeID;
    }


    public boolean isUnique() {
        return (this == UNIQUE || this == PRIMARY_KEY);
    }


    /**
     * Given the specified ID, this method returns the corresponding constraint
     * type enum value, or it returns <code>null</code> if the type value doesn't
     * signify any constraint type in this enumeration.
     */
    public static TableConstraintType findType(byte typeID) {
        for (TableConstraintType c : TableConstraintType.values()) {
            if (c.getTypeID() == typeID)
                return c;
        }
        return null;
    }
}
