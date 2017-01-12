package edu.caltech.nanodb.client.swingui;

import edu.caltech.nanodb.expressions.TupleLiteral;
import edu.caltech.nanodb.relations.Schema;

import javax.swing.table.AbstractTableModel;
import java.util.ArrayList;

/**
 * Created by IntelliJ IDEA.
 * User: donnie
 * Date: 12/27/11
 * Time: 10:48 AM
 * To change this template use File | Settings | File Templates.
 */
public class TupleTableModel extends AbstractTableModel {
    
    private Schema schema;


    private ArrayList<TupleLiteral> tuples = new ArrayList<TupleLiteral>();


    public void setSchema(Schema schema) {
        this.schema = schema;
        tuples.clear();

        fireTableStructureChanged();
    }


    public void addTuple(TupleLiteral tuple) {
        int row = tuples.size();
        tuples.add(tuple);

        fireTableRowsInserted(row, row);
    }


    public String getColumnName(int column) {
        return schema.getColumnInfo(column).getColumnName().toString();
    }


    @Override
    public int getRowCount() {
        return tuples.size();
    }


    @Override
    public int getColumnCount() {
        if (schema == null)
            return 0;

        return schema.numColumns();
    }


    @Override
    public Object getValueAt(int row, int column) {
        TupleLiteral t = tuples.get(row);

        Object value = t.getColumnValue(column);
        if (value == null)
            value = "NULL";

        return value;
    }
}
