package edu.caltech.nanodb.queryeval;


import java.io.PrintStream;

import edu.caltech.nanodb.relations.ColumnInfo;
import edu.caltech.nanodb.relations.Schema;
import edu.caltech.nanodb.relations.Tuple;


/**
 * This implementation of the tuple-processor interface simply prints out
 * the schema and tuples produced by the <tt>SELECT</tt> statement.
 */
public class TuplePrinter implements TupleProcessor {

    private PrintStream out;

    public TuplePrinter(PrintStream out) {
        this.out = out;
    }

    public void setSchema(Schema schema) {
        // TODO:  Print the schema.  Not like this.
        out.print("schema:  ");
        for (ColumnInfo colInfo : schema) {
            out.print(" | ");

            String colName = colInfo.getName();
            String tblName = colInfo.getTableName();

            // TODO:  To only print out table-names when the column-name is
            //        ambiguous by itself, uncomment the first part and
            //        then comment out the next part.

            // Only print out the table name if there are multiple columns
            // with this column name.
            // if (schema.numColumnsWithName(colName) > 1 && tblName != null)
            //     out.print(tblName + '.');

            // If table name is specified, always print it out.
            if (tblName != null)
                out.print(tblName + '.');

            out.print(colName);
        }
        out.println(" |");
    }

    public void process(Tuple tuple) {
        // TODO:  Print the tuple data.  Not like this.
        out.print("tuple:  ");
        for (int i = 0; i < tuple.getColumnCount(); i++) {
            out.print(" | ");
            out.print(tuple.getColumnValue(i));
        }
        out.println(" |");
    }

    public void finish() {
        // Not used
    }
}
