package edu.caltech.nanodb.queryeval;


import edu.caltech.nanodb.relations.SQLDataType;
import edu.caltech.nanodb.storage.TableManager;

import java.util.HashSet;


/**
 * This class facilitates the collection of statistics for a single column of a
 * table being analyzed by the {@link TableManager#analyzeTable}
 * method.  Instances of the class compute the number of distinct values, the
 * number of non-<tt>NULL</tt> values, and for appropriate data types, the
 * minimum and maximum values for the column.
 * <p>
 * The class also makes it very easy to construct a {@link ColumnStats} object
 * from the result of the analysis.
 *
 * @design (Donnie) This class is limited in its ability to efficiently compute
 *         the number of unique values for very large tables.  An
 *         external-memory approach would have to be used to support extremely
 *         large tables.
 */
public class ColumnStatsCollector {

    /** The SQL data-type for the column that stats are being collected for. */
    private SQLDataType sqlType;


    /**
     * The set of all values seen in this column.  This set could obviously
     * occupy a large amount of memory for large tables.
     */
    private HashSet<Object> uniqueValues;

    /**
     * A count of the number of <tt>NULL</tt> values seen in the column-values.
     */
    private int numNullValues;


    /**
     * The minimum value seen in the column's values, or <tt>null</tt> if the
     * minimum is unknown or won't be computed.
     */
    Comparable minValue;


    /**
     * The maximum value seen in the column's values, or <tt>null</tt> if the
     * maximum is unknown or won't be computed.
     */
    Comparable maxValue;


    /**
     * Initializes a new column-stats collector object for a column with the
     * specified base SQL datatype.
     *
     * @param sqlType the base SQL datatype for the column.
     */
    public ColumnStatsCollector(SQLDataType sqlType) {
        this.sqlType = sqlType;
        uniqueValues = new HashSet<Object>();
        numNullValues = 0;
        minValue = null;
        maxValue = null;
    }


    /**
     * Adds another column-value to this stats-collector object, updating the
     * statistics for the column.
     *
     * @param value the value from the column being analyzed.
     *
     * @design (Donnie) We have to suppress "unchecked operation" warnings on
     *         this code, since {@link Comparable} is a generic (and thus allows
     *         us to specify the type of object being compared), but we want to
     *         use it without specifying any types.
     */
    @SuppressWarnings("unchecked")
    public void addValue(Object value) {
        if (value == null) {
            numNullValues++;
        }
        else {
            // If the value implements the Comparable interface, use it to
            // update the minimum and maximum values.
            if (SelectivityEstimator.typeSupportsCompareEstimates(sqlType) &&
                value instanceof Comparable) {

                Comparable comp = (Comparable) value;

                if (minValue == null || comp.compareTo(minValue) < 0)
                    minValue = comp;

                if (maxValue == null || comp.compareTo(maxValue) > 0)
                    maxValue = comp;
            }

            // Update the set of unique values.
            uniqueValues.add(value);
        }
    }


    /**
     * Returns the number of <tt>NULL</tt> values seen for the column.
     *
     * @return the number of <tt>NULL</tt> values seen for the column
     */
    public int getNumNullValues() {
        return numNullValues;
    }


    /**
     * Returns the number of unique (and non-<tt>NULL</tt>) values seen for the
     * column.
     *
     * @return the number of unique (and non-<tt>NULL</tt>) values seen for the
     *         column
     */
    public int getNumUniqueValues() {
        return uniqueValues.size();
    }


    /**
     * Returns the minimum value seen for the column, or <tt>null</tt> if the
     * column's type isn't supported for comparison estimates (or if there
     * aren't any rows in the table being analyzed).
     *
     * @return the minimum value in the table for the column
     */
    public Object getMinValue() {
        return minValue;
    }


    /**
     * Returns the maximum value seen for the column, or <tt>null</tt> if the
     * column's type isn't supported for comparison estimates (or if there
     * aren't any rows in the table being analyzed).
     *
     * @return the maximum value in the table for the column
     */
    public Object getMaxValue() {
        return maxValue;
    }


    /**
     * This helper method constructs and returns a new column-statistics object
     * containing the stats collected by this object.
     *
     * @return a new column-stats object containing the stats that have been
     *         collected by this object
     */
    public ColumnStats getColumnStats() {
        return new ColumnStats(getNumUniqueValues(), numNullValues,
            minValue, maxValue);
    }
}
