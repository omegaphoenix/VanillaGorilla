package edu.caltech.nanodb.queryeval;


import java.io.PrintStream;

import edu.caltech.nanodb.relations.ColumnType;
import edu.caltech.nanodb.relations.SQLDataType;

import java.util.ArrayList;


/**
* Various methods to use for pretty-printing, such as delimiters, padding, and
* such.
*
* @author Angela Gong
*/
public class PrettyPrintTools {

    private PrintStream out;

    /** Stores the maximum column widths for each column **/
    private ArrayList<Integer> colWidths;

    /** Stores the column type for each column **/
    private ArrayList<ColumnType> colTypes;

    /** The number of columns to print **/
    private int numColumns;


    public PrettyPrintTools(PrintStream out, ArrayList<Integer> colWidths,
        ArrayList<ColumnType> colTypes, int numColumns) {
        this.out = out;
        this.colWidths = colWidths;
        this.colTypes = colTypes;
        this.numColumns = numColumns;
    }

    /**
     * Prints the delimiter, i.e. the "+-------+--------+" portion.
     */
    public void printDelimiter() {
        // Print the columns
        for (int i = 0; i < numColumns; i++) {
            out.print("+");
            // Print each individual column
            for (int j = 0; j < getColumnWidth(i); j++) {
                out.print("-");
            }
        }
        out.print("+\n");
    }


    /**
     * Gets the width of a specified column.
     *
     * @param index the index of the colujmn
     * @return the column width
     */
    public int getColumnWidth(int index) {
        Integer width = colWidths.get(index);
        // Add 2 for the spaces on either side
        return width.intValue() + 2;
    }


    /**
     * Pads a string with the spaces on either end necessary when printing.
     *
     * @param colWidth the width of the column the string will be printed in
     * @param str the string to be printed
     * @return the newly-padded string
     */
    public String padString(Integer colWidth, String str) {
        int size = colWidth.intValue();
        int strLen = str.length();
        StringBuilder paddedString = new StringBuilder(" " + str);

        for (int i = 0; i < size - strLen - 1; i++) {
            paddedString.append(" ");
        }
        return paddedString.toString();
    }


    /**
     * Pads a string with the spaces on either end necessary when printing.
     * Also handles if tuples are numbers, in that case, the column is aligned
     * right instead of left
     *
     * @param colWidth the width of the column the string will be printed in
     * @param str the string to be printed
     * @param index the index of the column being printed
     * @return the newly-padded string
     */
    public String padString(Integer colWidth, String str, int index) {
        int size = colWidth.intValue();
        int strLen = str.length();
        StringBuilder paddedString = new StringBuilder();

        SQLDataType type = colTypes.get(index).getBaseType();
        switch (type) {
            // If one of the numeric types, align them to the right
            case INTEGER:
            case SMALLINT:
            case BIGINT:
            case TINYINT:
            case FLOAT:
            case DOUBLE:
            case NUMERIC:
                for (int i = 0; i < size - strLen - 1; i++) {
                    paddedString.append(" ");
                }
                paddedString.append(str + " ");
                return paddedString.toString();

            // Otherwise, pad normally
            default: return padString(colWidth, str);
        }
    }


    /**
     * Changes the maximum column width stored if necessary
     *
     * @param str the string added to the list of things to print
     * @param col the column it was added at
     */
    public void checkColumnWidths(String str, int col) {
        if (colWidths.get(col) < str.length())
                colWidths.set(col, str.length());
    }
}
