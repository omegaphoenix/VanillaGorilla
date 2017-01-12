package edu.caltech.test.nanodb.relations;


import edu.caltech.nanodb.relations.ColumnRefs;
import org.testng.annotations.Test;


/**
 * This test class exercises the functionality of the
 * {@link ColumnRefs} class.
 **/
@Test
public class TestColumnIndexes {

    public void testCtorNegatives() {
        try {
            ColumnRefs c = new ColumnRefs(new int[] {1, 2, 3, -4});
            assert false;
        }
        catch (IllegalArgumentException e) {
            // This is expected.
        }

        try {
            ColumnRefs c = new ColumnRefs(new int[] {-1, 2, 3, 4});
            assert false;
        }
        catch (IllegalArgumentException e) {
            // This is expected.
        }

        try {
            ColumnRefs c = new ColumnRefs(new int[] {1, 2, -3, 4});
            assert false;
        }
        catch (IllegalArgumentException e) {
            // This is expected.
        }
    }


    public void testCtorDuplicates() {
        try {
            ColumnRefs c = new ColumnRefs(new int[] {1, 2, 3, 1});
            assert false;
        }
        catch (IllegalArgumentException e) {
            // This is expected.
        }

        try {
            ColumnRefs c = new ColumnRefs(new int[] {1, 2, 2, 4});
            assert false;
        }
        catch (IllegalArgumentException e) {
            // This is expected.
        }

        try {
            ColumnRefs c = new ColumnRefs(new int[] {1, 2, 1, 4});
            assert false;
        }
        catch (IllegalArgumentException e) {
            // This is expected.
        }

        try {
            ColumnRefs c = new ColumnRefs(new int[] {1, 4, 3, 4});
            assert false;
        }
        catch (IllegalArgumentException e) {
            // This is expected.
        }
    }


    public void testSimpleAccess() {
        ColumnRefs c = new ColumnRefs(new int[] {5, 3, 2, 4, 1});
        assert c.size() == 5;

        assert c.getCol(0) == 5;
        assert c.getCol(1) == 3;
        assert c.getCol(2) == 2;
        assert c.getCol(3) == 4;
        assert c.getCol(4) == 1;
    }


    public void testEqualsColumns() {
        ColumnRefs c1 = new ColumnRefs(new int[] {3, 1, 4});
        ColumnRefs c2 = new ColumnRefs(new int[] {1, 3, 4});
        ColumnRefs c3 = new ColumnRefs(new int[] {3, 1, 4});

        assert !c1.equalsColumns(c2);
        assert !c2.equalsColumns(c1);
        assert !c2.equalsColumns(c3);

        assert c1.equalsColumns(c3);
        assert c3.equalsColumns(c1);
    }


    public void testHasSameColumns() {
        ColumnRefs c1 = new ColumnRefs(new int[] {3, 1, 4});
        ColumnRefs c2 = new ColumnRefs(new int[] {1, 3, 4});
        ColumnRefs c3 = new ColumnRefs(new int[] {4, 3, 1});
        ColumnRefs c4 = new ColumnRefs(new int[] {3, 2, 4, 1});
        ColumnRefs c5 = new ColumnRefs(new int[] {1, 3, 4, 2});

        assert c1.hasSameColumns(c2);
        assert c1.hasSameColumns(c3);
        assert c2.hasSameColumns(c1);
        assert c2.hasSameColumns(c3);
        assert c3.hasSameColumns(c1);
        assert c3.hasSameColumns(c2);

        assert c4.hasSameColumns(c5);
        assert c5.hasSameColumns(c4);

        assert !c1.hasSameColumns(c4);
        assert !c5.hasSameColumns(c3);
    }
}

