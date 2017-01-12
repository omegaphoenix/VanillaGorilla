package edu.caltech.nanodb.relations;


import java.util.Arrays;
import java.util.HashSet;


/**
 * This class represents a set of columns in a schema by specifying the
 * indexes of the columns in the set.  This is used to represent primary keys,
 * candidate keys, foreign keys, and columns in indexes.
 */
public class ColumnRefs {
    /**
     * This is the actual name for referring to the index.  It is mapped to
     * an index filename, where the index is actually stored.
     */
    private String indexName;


    /** This array holds the indexes of the columns in the set. */
    private int[] colIndexes;


    /**
     * If this index was created to enforce a particular table constraint,
     * this field specifies the kind of constraint it is used to enforce.
     */
    private TableConstraintType constraintType;


    protected ColumnRefs(String indexName, int[] colIndexes,
                      TableConstraintType constraintType) {
        if (colIndexes == null)
            throw new IllegalArgumentException("colIndexes must be specified");

        if (colIndexes.length == 0) {
            throw new IllegalArgumentException(
                "colIndexes must have at least one element");
        }

        this.indexName = indexName;

        // Make sure that no column-index values are duplicated, and that none
        // are negative values.
        int[] tmp = colIndexes.clone();
        Arrays.sort(tmp);
        for (int i = 0; i < tmp.length; i++) {
            if (tmp[i] < 0) {
                throw new IllegalArgumentException(
                    "colIndexes cannot contain negative values; got " +
                    Arrays.toString(colIndexes));
            }

            if (i > 0 && tmp[i] == tmp[i - 1]) {
                throw new IllegalArgumentException(
                    "colIndexes cannot contain duplicate values; got " +
                    Arrays.toString(colIndexes));
            }
        }

        this.colIndexes = colIndexes;

        this.constraintType = constraintType;
    }


    public ColumnRefs(int[] colIndexes) {
        this(null, colIndexes, null);
    }


    public ColumnRefs(String indexName, int[] colIndexes) {
        this(indexName, colIndexes, null);
    }


    public int size() {
        return colIndexes.length;
    }


    public int getCol(int i) {
        return colIndexes[i];
    }

    public int[] getCols() {
        return colIndexes;
    }


    /**
     * Returns true if the specified <tt>ColumnIndexes</tt> object has the
     * same columns as this object, in the exact same order.
     *
     * @param ci the <tt>ColumnIndexes</tt> object to compare to this object
     *
     * @return true if the two objects have the same column indexes, in the
     *         exact same order
     */
    public boolean equalsColumns(ColumnRefs ci) {
        return Arrays.equals(colIndexes, ci.colIndexes);
    }


    /**
     * Returns true if the specified <tt>ColumnIndexes</tt> object has the same
     * columns as this object, independent of order.
     *
     * @param ci the <tt>ColumnIndexes</tt> object to compare to this object
     *
     * @return true if the two objects have the same column indexes,
     *         independent of order
     */
    public boolean hasSameColumns(ColumnRefs ci) {
        HashSet<Integer> indexes = new HashSet<Integer>();
        for (int i : colIndexes)
            indexes.add(i);

        for (int i : ci.colIndexes) {
            if (!indexes.contains(i))
                return false;

            indexes.remove(i);
        }

        return indexes.isEmpty();
    }


    public String getIndexName() {
        return indexName;
    }


    public void setIndexName(String indexName) {
        this.indexName = indexName;
    }


    public void setConstraintType(TableConstraintType constraintType) {
        this.constraintType = constraintType;
    }


    public TableConstraintType getConstraintType() {
        return constraintType;
    }


    @Override
    public String toString() {
        StringBuilder buf = new StringBuilder();

        buf.append('(');
        boolean first = true;
        for (int i : colIndexes) {
            if (first)
                first = false;
            else
                buf.append(", ");

            buf.append(i);
        }
        buf.append(')');

        return buf.toString();
    }
}
