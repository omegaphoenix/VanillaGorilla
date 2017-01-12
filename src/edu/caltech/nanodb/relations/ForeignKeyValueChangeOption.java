package edu.caltech.nanodb.relations;


/** 
 * An enumeration specifying the options allowed for <tt>ON DELETE</tt> and
 * <tt>ON UPDATE</tt> actions for foreign keys.
 */
public enum ForeignKeyValueChangeOption {

    /** Rejects the delete or update command on the parent table */
    RESTRICT(0),

    /**
     * Delete or update the row from the parent table and automatically
     * delete/update the matching rows in the child table
     */
    CASCADE(1),

    /**
     * Delete or update the row from the parent table and set the foreign key
     * column(s) in the child table to <tt>NULL</tt>
     */
    SET_NULL(2);


    /** The ID of the option type. */
    private final byte typeID;


    /**
     * Construct a new @code{ForeignKeyValueChangeOption} instance with the
     * specified option ID.  Each option should have a unique ID.  Note that
     * this constructor is private, since this is the only place where foreign
     * option types should be declared.
     */
    private ForeignKeyValueChangeOption(int typeID) {
        this.typeID = (byte) typeID;
    }


    /** Returns the option's unique ID. */
    public byte getTypeID() {
        return typeID;
    }


    /**
     * Given the specified ID, this method returns the corresponding constraint
     * type enum value, or it returns <code>null</code> if the type value doesn't
     * signify any constraint type in this enumeration.
     */
    public static ForeignKeyValueChangeOption findType(int typeID) {
        for (ForeignKeyValueChangeOption f : ForeignKeyValueChangeOption.values()) {
            if (f.getTypeID() == typeID)
                return f;
        }
        return null;
    }
}
