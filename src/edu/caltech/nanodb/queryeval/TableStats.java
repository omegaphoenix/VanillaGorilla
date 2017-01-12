package edu.caltech.nanodb.queryeval;


import java.util.ArrayList;


/** This class is a simple wrapper for table-file statistics. */
public class TableStats {

    /**
     * The total number of data pages in the table file.  This number is in the
     * range of [0, 2<sup>16</sup>).
     */
    public int numDataPages;


    /**
     * The total number of tuples in the table file.  This number is in the
     * range of [0, 2<sup>31</sup> - 1).
     */
    public int numTuples;


    /**
     * The average number of bytes in tuples in this table file.  This value is
     * a float, and usually includes a fractional part.
     */
    public float avgTupleSize;


    /**
     * This collection holds statistics about individual columns in the table.
     */
    private ArrayList<ColumnStats> columnStats;


    /**
     * Create a new table-statistics object with the stats set to the specified
     * values.  The array of column-statistics objects must have the same number
     * of elements as the table's schema has columns.
     *
     * @param numDataPages the number of data pages in the table file
     * @param numTuples the number of tuples in the table file
     * @param avgTupleSize the average tuple-size in bytes
     * @param columnStats an array of column-statistics generated from the table
     */
    public TableStats(int numDataPages, int numTuples, float avgTupleSize,
                      ArrayList<ColumnStats> columnStats) {
        if (columnStats == null)
            throw new IllegalArgumentException("columnStats cannot be null");

        this.numDataPages = numDataPages;
        this.numTuples = numTuples;
        this.avgTupleSize = avgTupleSize;

        this.columnStats = columnStats;
    }


    /**
     * Create a new table-statistics object with all statistics initialized to
     * zero or <tt>NULL</tt> values.
     *
     * @param numColumns the number of columns in the table
     */
    public TableStats(int numColumns) {
        numDataPages = 0;
        numTuples = 0;
        avgTupleSize = 0;

        columnStats = new ArrayList<ColumnStats>(numColumns);
        for (int i = 0; i < numColumns; i++)
            columnStats.add(new ColumnStats());
    }


    /**
     * Returns the column-statistics for the specified column.
     *
     * @param index the index of the column to retrieve the stats for
     *
     * @return the column-stats object for the specified column
     */
    public ColumnStats getColumnStats(int index) {
        return columnStats.get(index);
    }


    /**
     * Returns the collection of all column-statistics collected for this table.
     *
     * @return a collection of all column-statistics for the table
     */
    public ArrayList<ColumnStats> getAllColumnStats() {
        return columnStats;
    }


    @Override
    public String toString() {
      return "TableStats[numDataPages=" + numDataPages + ", numTuples=" +
        numTuples + ", avgTupleSize=" + avgTupleSize + "]";
    }
}
