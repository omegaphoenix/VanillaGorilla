package edu.caltech.nanodb.queryeval;


import java.io.PrintStream;

import edu.caltech.nanodb.relations.ColumnInfo;
import edu.caltech.nanodb.relations.ColumnType;
import edu.caltech.nanodb.relations.Schema;
import edu.caltech.nanodb.relations.Tuple;

import java.util.ArrayList;


/**
* This implementation of the tuple-processor interface prints out the schema
* and the tuples produced by the <tt>SELECT</tt> statement in a cooler,
* prettier fashion.
*
* @author Angela Gong
*/
public class PrettyTuplePrinter implements TupleProcessor {

    private PrintStream out;

    /** Contains the schema to be printed **/
    private ArrayList<String> schema;

    /** Contains all the tuples to be printed **/
    private ArrayList<String> tuples;

    /** Stores the maximum column widths for each column **/
    private ArrayList<Integer> colWidths;

    /** Stores the column type for each column **/
    private ArrayList<ColumnType> colTypes;

    /** The number of columns to print **/
    private int numColumns;

    /** Tools to aid in pretty printing **/
    PrettyPrintTools pTools;


    public PrettyTuplePrinter(PrintStream out) {
        this.out = out;
        schema = new ArrayList<String>();
        tuples = new ArrayList<String>();
        colWidths = new ArrayList<Integer>();
        colTypes = new ArrayList<ColumnType>();
    }


    public void setSchema(Schema schema) {
        int i = 0;

        for (ColumnInfo colInfo : schema) {
            String schemaName = "";

            String colName = colInfo.getName();
            String tblName = colInfo.getTableName();

            // NOTE:  To only print out table-names when the column-name is
            //        ambiguous by itself, uncomment the first part and
            //        then comment out the next part.

            // Only print out the table name if there are multiple columns
            // with this column name.
            if (schema.numColumnsWithName(colName) > 1 && tblName != null)
                 schemaName += tblName + ".";

            // If table name is specified, always print it out.
            // if (tblName != null)
            //     schemaName += tblName + ".";

            schemaName += colName;

            // Add to the list of column widths
            colWidths.add(i, schemaName.length());
            this.schema.add(schemaName);
            colTypes.add(colInfo.getType());

            i++;
        }

        numColumns = colWidths.size();
        // Initialize a pretty printing tool
        pTools = new PrettyPrintTools(out, colWidths, colTypes, numColumns);
    }


    public void process(Tuple tuple) {
        // Process tuples but don't actually print them yet

        for (int i = 0; i < tuple.getColumnCount(); i++) {
            String tupleName = "";
            tupleName += tuple.getColumnValue(i);

            // If this tuple's width is larger than the current largest width of
            // the column, then change it
            pTools.checkColumnWidths(tupleName, i);

            tuples.add(tupleName);
        }
    }


    public void finish() {
        Integer colWidth;
        String toPrint;

        int numTuples = tuples.size() / numColumns;
        // If there are no tuples to print, report this and just return
        if (tuples.isEmpty()) {
            out.print("No results\n\n");
            return;
        }

        pTools.printDelimiter();

        // Print the schema
        for (int i = 0; i < numColumns; i++) {
            out.print("|");
            colWidth = pTools.getColumnWidth(i);
            toPrint = schema.get(i);
            out.print(pTools.padString(colWidth, toPrint));
        }
        out.print("|\n");

        pTools.printDelimiter();

        // Print the tuples
        for (int i = 0; i < numTuples; i++) {
            // Print each column in the tuple
            for (int j = i * numColumns; j < i * numColumns + numColumns; j++) {
                out.print("|");
                colWidth = pTools.getColumnWidth(j % numColumns);
                toPrint = tuples.get(j);
                out.print(pTools.padString(colWidth, toPrint, j % numColumns));
            }
            out.print("|\n");
        }

        pTools.printDelimiter();
        out.print("\n");
    }
}
