package edu.caltech.nanodb.queryeval;


/**
 * This class holds some useful statistics for a specific column.  At present
 * this consists of the following:
 * <ul>
 *   <li>the number of unique values in the column (not including <tt>NULL</tt>
 *       in the count)</li>
 *   <li>the number of <tt>NULL</tt> values in the column</li>
 *   <li>the minimum value for the column</li>
 *   <li>the maximum value for the column</li>
 * </ul>
 * The {@link ColumnStatsCollector} class can be used to easily collect these
 * statistics for a particular column of a table.
 */
public class ColumnStats {
    /**
     * The total number of unique values for this column in the table, or -1 if
     * the total number of unique values is unknown.
     */
    private int numUniqueValues;


    /**
     * The total number of <tt>NULL</tt> values for this column in the table,
     * or -1 if the total number of <tt>NULL</tt> values is unknown.
     */
    private int numNullValues;


    /**
     * The minimum value of this column in the table, or <tt>null</tt> if the
     * minimum value is unknown.
     */
    private Object minValue;


    /**
     * The maximum value of this column in the table, or <tt>null</tt> if the
     * maximum value is unknown.
     */
    private Object maxValue;


    /** Initializes a column-stats object to all "unknown" values. */
    public ColumnStats() {
        numUniqueValues = -1;
        numNullValues = -1;
        minValue = null;
        maxValue = null;
    }


    /**
     * Initializes a column-stats object with the specified values.
     *
     * @param numUniqueValues the number of unique values in the column, or -1
     *        if unknown
     * @param numNullValues the number of <tt>NULL</tt> values in the column, or
     *        -1 if unknown
     * @param minValue the minimum value in the column, or <tt>null</tt> if
     *        unknown
     * @param maxValue the maximum value in the column, or <tt>null</tt> if
     *        unknown
     */
    public ColumnStats(int numUniqueValues, int numNullValues,
                       Object minValue, Object maxValue) {
        setNumUniqueValues(numUniqueValues);
        setNumNullValues(numNullValues);
        setMinValue(minValue);
        setMaxValue(maxValue);
    }


    /**
     * Returns the number of unique values for the column, or -1 if the number
     * is unknown
     *
     * @return the number of unique values for the column, or -1 if the number
     *         is unknown
     */
    public int getNumUniqueValues() {
        return numUniqueValues;
    }


    /**
     * Sets the number of unique values for the column.
     *
     * @param num the number of unique values in the table for the column, or
     *        -1 if the number if unknown
     */
    public void setNumUniqueValues(int num) {
        if (num < -1) {
            throw new IllegalArgumentException(
                "Number of unique values must be >= -1; got " + num);
        }

        numUniqueValues = num;
    }


    /**
     * Returns the number of <tt>NULL</tt> values for the column, or -1 if the
     * number is unknown
     *
     * @return the number of <tt>NULL</tt> values for the column, or -1 if the
     *         number is unknown
     */
    public int getNumNullValues() {
        return numNullValues;
    }


    /**
     * Sets the number of <tt>NULL</tt> values for the column.
     *
     * @param num the number of <tt>NULL</tt> values in the table for the
     *        column, or -1 if the number if unknown
     */
    public void setNumNullValues(int num) {
        if (num < -1) {
            throw new IllegalArgumentException(
                "Number of NULL values must be >= -1; got " + num);
        }

        numNullValues = num;
    }



    /**
     * Returns the minimum value for the column.
     *
     * @return the minimum value in the table for the column
     */
    public Object getMinValue() {
        return minValue;
    }


    /**
     * Sets the minimum value for the column.
     *
     * @param minValue the minimum value in the table for the column
     */
    public void setMinValue(Object minValue) {
        this.minValue = minValue;
    }


    /**
     * Returns the maximum value for the column.
     *
     * @return the maximum value in the table for the column
     */
    public Object getMaxValue() {
        return maxValue;
    }


    /**
     * Sets the maximum value for the column.
     *
     * @param maxValue the maximum value in the table for the column
     */
    public void setMaxValue(Object maxValue) {
        this.maxValue = maxValue;
    }


    /**
     * Returns <tt>true</tt> if this column-stats object has both minimum and
     * maximum values.
     *
     * @return <tt>true</tt> if this column-stats object has both minimum and
     *         maximum values
     */
    public boolean hasMinMaxValues() {
        return (minValue != null && maxValue != null);
    }


    /**
     * Returns <tt>true</tt> if this column-stats object has both minimum and
     * maximum values, and they are actually different values.
     *
     * @return <tt>true</tt> if this column-stats object has both minimum and
     *         maximum values, and they are actually different values.
     */
    public boolean hasDifferentMinMaxValues() {
        return hasMinMaxValues() && (!minValue.equals(maxValue));
    }
}
