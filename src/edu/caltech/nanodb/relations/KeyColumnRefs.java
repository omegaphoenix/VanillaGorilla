package edu.caltech.nanodb.relations;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;


/**
 * This class represents a primary key or other unique key, specifying the
 * indexes of the columns in the key.  The class also specifies the index
 * used to enforce the key in the database.
 */
public class KeyColumnRefs extends ColumnRefs {

    /**
     * This nested class simply records a referencing table and index that
     * refer to a particular candidate key on another table.
     */
    public static class FKReference {
        public String tableName;

        public String indexName;

        public FKReference(String tableName, String indexName) {
            this.tableName = tableName;
            this.indexName = indexName;
        }


        @Override
        public boolean equals(Object obj) {
            if (obj instanceof FKReference) {
                FKReference other = (FKReference) obj;
                return (tableName.equals(other.tableName) &&
                        indexName.equals(other.indexName));
            }

            return false;
        }


        @Override
        public int hashCode() {
            return tableName.hashCode() * indexName.hashCode();
        }
    }


    /** This is the optional name of the constraint specified in the DDL. */
    private String constraintName;


    /**
     * List of foreign-key references to this index, specified as (table,
     * index) pairs.
     */
    private ArrayList<FKReference> foreignKeyReferences;


    public KeyColumnRefs(String indexName, int[] colIndexes,
                         TableConstraintType constraintType) {
        super(indexName, colIndexes, constraintType);

        if (constraintType != TableConstraintType.PRIMARY_KEY &&
            constraintType != TableConstraintType.UNIQUE) {
            throw new IllegalArgumentException("constraintType must be " +
                "PRIMARY_KEY or UNIQUE, got " + constraintType);
        }

        foreignKeyReferences = new ArrayList<FKReference>();
    }

/*
    public KeyColumnRefs(int[] colIndexes) {
        this(null, colIndexes);
    }
*/

    public String getConstraintName() {
        return constraintName;
    }


    public void setConstraintName(String constraintName) {
        this.constraintName = constraintName;
    }


    public void dropRefToTable(String tblName) {
        Iterator<FKReference> iter = foreignKeyReferences.iterator();
        while (iter.hasNext()) {
            FKReference ref = iter.next();
            if (ref.tableName.equals(tblName))
                iter.remove();
        }
    }


    public void addRef(String tblName, String idxName) {
        this.foreignKeyReferences.add(new FKReference(tblName, idxName));
    }


    public List<FKReference> getReferencingIndexes() {
        return Collections.unmodifiableList(foreignKeyReferences);
    }
}
