package edu.caltech.nanodb.storage.writeahead;


/**
 * This enumeration specifies the various types of records that can appear in
 * the write-ahead log, along with their numeric values that actually appear
 * within the write-ahead log.
 */
public enum WALRecordType {
    /**
     * The record represents a "&lt;<i>T<sub>i</sub></i>:  start
     * transaction&gt;" record.
     */
    START_TXN(1),

    /**
     * The record represents a "&lt;<i>T<sub>i</sub></i>:  update <i>P</i>
     * &rarr; <i>P'</i> &gt;" record.
     */
    UPDATE_PAGE(2),

    /**
     * The record represents a "&lt;<i>T<sub>i</sub></i>:  update <i>P'</i>
     * (redo only)&gt;" record.
     */
    UPDATE_PAGE_REDO_ONLY(3),

    /**
     * The record represents a "&lt;<i>T<sub>i</sub></i>:  commit
     * transaction&gt;" record.
     */
    COMMIT_TXN(10),

    /**
     * The record represents a "&lt;<i>T<sub>i</sub></i>:  abort
     * transaction&gt;" record.
     */
    ABORT_TXN(11);


    private int id;


    private WALRecordType(int id) {
        this.id = id;
    }


    public int getID() {
        return id;
    }


    /**
     * Given a numeric type ID, returns the corresponding type value for the ID,
     * or <tt>null</tt> if no type corresponds to the ID.
     *
     * @param id the numeric ID of the type to retrieve
     *
     * @return the type-value with that ID, or <tt>null</tt> if not found
     */
    public static WALRecordType valueOf(int id) {
        for (WALRecordType type : values()) {
            if (type.id == id)
                return type;
        }
        return null;
    }
}
