package edu.caltech.test.nanodb.expressions;


import org.testng.annotations.*;

import edu.caltech.nanodb.expressions.ColumnName;


/**
 * This test class exercises the functionality of the
 * {@link edu.caltech.nanodb.expressions.ColumnName} class.
 */
@Test
public class TestColumnName {

    /**
     * This test exercises the
     * {@link edu.caltech.nanodb.expressions.ColumnName#isTableSpecified} and
     * {@link edu.caltech.nanodb.expressions.ColumnName#isColumnWildcard} methods.
     */
    public void testColumnWildcards() {
        ColumnName cn1 = new ColumnName();            // Wildcard:  *
        ColumnName cn2 = new ColumnName("col1");      // Column "col1" (no table)
        ColumnName cn3 = new ColumnName("col2");
        ColumnName cn4 = new ColumnName("tbl1", null);    // Wildcard:  tbl1.*
        ColumnName cn5 = new ColumnName("tbl1", "col1");  // tbl1.col1
        ColumnName cn6 = new ColumnName("tbl1", "col2");  // tbl1.col2
        ColumnName cn7 = new ColumnName("tbl2", "col1");  // tbl2.col1

        assert !cn1.isTableSpecified();
        assert cn1.isColumnWildcard();

        assert !cn2.isTableSpecified();
        assert !cn2.isColumnWildcard();

        assert !cn3.isTableSpecified();
        assert !cn3.isColumnWildcard();

        assert cn4.isTableSpecified();
        assert cn4.isColumnWildcard();

        assert cn5.isTableSpecified();
        assert !cn5.isColumnWildcard();

        assert cn6.isTableSpecified();
        assert !cn6.isColumnWildcard();

        assert cn7.isTableSpecified();
        assert !cn7.isColumnWildcard();
    }


    /** This test exercises the {@link java.lang.Comparable#compareTo} method. */
    public void testCompareOrder() {
        ColumnName cn1 = new ColumnName();            // Wildcard:  *
        ColumnName cn2 = new ColumnName("col1");      // Column "col1" (no table)
        ColumnName cn3 = new ColumnName("col2");
        ColumnName cn4 = new ColumnName("tbl1", null);    // Wildcard:  tbl1.*
        ColumnName cn5 = new ColumnName("tbl1", "col1");  // tbl1.col1
        ColumnName cn6 = new ColumnName("tbl1", "col2");  // tbl1.col2
        ColumnName cn7 = new ColumnName("tbl2", "col1");  // tbl2.col1

        assert cn1.compareTo(cn1) == 0;
        assert cn1.compareTo(cn2) < 0;
        assert cn1.compareTo(cn3) < 0;
        assert cn1.compareTo(cn4) < 0;
        assert cn1.compareTo(cn5) < 0;
        assert cn1.compareTo(cn6) < 0;
        assert cn1.compareTo(cn7) < 0;

        assert cn2.compareTo(cn1) > 0;
        assert cn2.compareTo(cn2) == 0;
        assert cn2.compareTo(cn3) < 0;
        assert cn2.compareTo(cn4) < 0;
        assert cn2.compareTo(cn5) < 0;
        assert cn2.compareTo(cn6) < 0;
        assert cn2.compareTo(cn7) < 0;

        assert cn3.compareTo(cn1) > 0;
        assert cn3.compareTo(cn2) > 0;
        assert cn3.compareTo(cn3) == 0;
        assert cn3.compareTo(cn4) < 0;
        assert cn3.compareTo(cn5) < 0;
        assert cn3.compareTo(cn6) < 0;
        assert cn3.compareTo(cn7) < 0;

        assert cn4.compareTo(cn1) > 0;
        assert cn4.compareTo(cn2) > 0;
        assert cn4.compareTo(cn3) > 0;
        assert cn4.compareTo(cn4) == 0;
        assert cn4.compareTo(cn5) < 0;
        assert cn4.compareTo(cn6) < 0;
        assert cn4.compareTo(cn7) < 0;

        assert cn5.compareTo(cn1) > 0;
        assert cn5.compareTo(cn2) > 0;
        assert cn5.compareTo(cn3) > 0;
        assert cn5.compareTo(cn4) > 0;
        assert cn5.compareTo(cn5) == 0;
        assert cn5.compareTo(cn6) < 0;
        assert cn5.compareTo(cn7) < 0;

        assert cn6.compareTo(cn1) > 0;
        assert cn6.compareTo(cn2) > 0;
        assert cn6.compareTo(cn3) > 0;
        assert cn6.compareTo(cn4) > 0;
        assert cn6.compareTo(cn5) > 0;
        assert cn6.compareTo(cn6) == 0;
        assert cn6.compareTo(cn7) < 0;

        assert cn7.compareTo(cn1) > 0;
        assert cn7.compareTo(cn2) > 0;
        assert cn7.compareTo(cn3) > 0;
        assert cn7.compareTo(cn4) > 0;
        assert cn7.compareTo(cn5) > 0;
        assert cn7.compareTo(cn6) > 0;
        assert cn7.compareTo(cn7) == 0;
    }


    /**
     * This test exercises the
     * {@link edu.caltech.nanodb.expressions.ColumnName#toString} method.
     */
    public void testToString() {
        ColumnName cn1 = new ColumnName();            // Wildcard:  *
        ColumnName cn2 = new ColumnName("col1");      // Column "col1" (no table)
        ColumnName cn3 = new ColumnName("col2");
        ColumnName cn4 = new ColumnName("tbl1", null);    // Wildcard:  tbl1.*
        ColumnName cn5 = new ColumnName("tbl1", "col1");  // tbl1.col1
        ColumnName cn6 = new ColumnName("tbl1", "col2");  // tbl1.col2
        ColumnName cn7 = new ColumnName("tbl2", "col1");  // tbl2.col1

        assert         "*".equals(cn1.toString());
        assert      "col1".equals(cn2.toString());
        assert      "col2".equals(cn3.toString());
        assert    "tbl1.*".equals(cn4.toString());
        assert "tbl1.col1".equals(cn5.toString());
        assert "tbl1.col2".equals(cn6.toString());
        assert "tbl2.col1".equals(cn7.toString());
    }
}
