package edu.caltech.nanodb.storage;


import java.util.ArrayList;

import org.apache.log4j.Logger;

import edu.caltech.nanodb.queryeval.ColumnStats;
import edu.caltech.nanodb.queryeval.TableStats;
import edu.caltech.nanodb.relations.ColumnInfo;
import edu.caltech.nanodb.relations.Schema;


/**
 * Created by donnie on 2/27/14.
 */
public class StatsWriter {
    /** A logging object for reporting anything interesting that happens. */
    private static Logger logger = Logger.getLogger(StatsWriter.class);


    /**
     * A bit-mask used for storing column-stats, to record whether or not the
     * "number of distinct values" value is present for the column.
     */
    private static final int COLSTAT_NULLMASK_NUM_DISTINCT_VALUES = 0x08;


    /**
     * A bit-mask used for storing column-stats, to record whether or not the
     * "number of <tt>NULL</tt> values" value is present for the column.
     */
    private static final int COLSTAT_NULLMASK_NUM_NULL_VALUES = 0x04;


    /**
     * A bit-mask used for storing column-stats, to record whether or not the
     * "minimum value" value is present for the column.
     */
    private static final int COLSTAT_NULLMASK_MIN_VALUE = 0x02;


    /**
     * A bit-mask used for storing column-stats, to record whether or not the
     * "maximum value" value is present for the column.
     */
    private static final int COLSTAT_NULLMASK_MAX_VALUE = 0x01;


    public void writeTableStats(Schema schema, TableStats stats, PageWriter pgWriter) {
        logger.debug("Writing table-statistics:  " + stats);

        int startPosition = pgWriter.getPosition();

        pgWriter.writeShort(stats.numDataPages);
        pgWriter.writeInt(stats.numTuples);
        pgWriter.writeFloat(stats.avgTupleSize);

        ArrayList<ColumnStats> colStats = stats.getAllColumnStats();
        for (int i = 0; i < colStats.size(); i++) {
            ColumnStats c = colStats.get(i);
            ColumnInfo colInfo = schema.getColumnInfo(i);

            // There are three values per column-stat, and any of them can be
            // null.  Therefore, each column-stat gets its own NULL-mask.
            byte nullMask = 0;

            int numUnique = c.getNumUniqueValues();
            int numNull   = c.getNumNullValues();
            Object minVal = c.getMinValue();
            Object maxVal = c.getMaxValue();

            // Build up the NULL-mask.

            if (numUnique == -1)
                nullMask |= COLSTAT_NULLMASK_NUM_DISTINCT_VALUES;

            if (numNull == -1)
                nullMask |= COLSTAT_NULLMASK_NUM_NULL_VALUES;

            if (minVal == null)
                nullMask |= COLSTAT_NULLMASK_MIN_VALUE;

            if (maxVal == null)
                nullMask |= COLSTAT_NULLMASK_MAX_VALUE;

            // Store the NULL-mask, then store the non-NULL values.

            logger.debug(String.format("Writing column-stat data:  " +
                                           "nullmask=0x%X, unique=%d, null=%d, min=%s, max=%s",
                                          nullMask, numUnique, numNull, minVal, maxVal));

            pgWriter.writeByte(nullMask);

            if (numUnique != -1)
                pgWriter.writeInt(numUnique);

            if (numNull != -1)
                pgWriter.writeInt(numNull);

            if (minVal != null)
                pgWriter.writeObject(colInfo.getType(), minVal);

            if (maxVal != null)
                pgWriter.writeObject(colInfo.getType(), maxVal);
        }

        if (logger.isDebugEnabled()) {
            int size = pgWriter.getPosition() - startPosition;
            logger.debug("Table statistics occupy " + size + " bytes.");
        }
    }


    public TableStats readTableStats(PageReader pgReader, Schema schema) {
        logger.debug("Reading table-statistics.");

        int numDataPages = pgReader.readUnsignedShort();
        int numTuples = pgReader.readInt();
        float avgTupleSize = pgReader.readFloat();

        ArrayList<ColumnStats> colStats = new ArrayList<ColumnStats>();
        for (int i = 0; i < schema.numColumns(); i++) {
            // The column-statistics object is initialized to all NULL values.
            ColumnStats c = new ColumnStats();
            ColumnInfo colInfo = schema.getColumnInfo(i);

            // Read each column-stat's NULL-mask.  If the null-bit is 0 then a
            // value is present.
            byte nullMask = pgReader.readByte();

            if ((nullMask & COLSTAT_NULLMASK_NUM_DISTINCT_VALUES) == 0)
                c.setNumUniqueValues(pgReader.readInt());

            if ((nullMask & COLSTAT_NULLMASK_NUM_NULL_VALUES) == 0)
                c.setNumNullValues(pgReader.readInt());

            if ((nullMask & COLSTAT_NULLMASK_MIN_VALUE) == 0)
                c.setMinValue(pgReader.readObject(colInfo.getType()));

            if ((nullMask & COLSTAT_NULLMASK_MAX_VALUE) == 0)
                c.setMaxValue(pgReader.readObject(colInfo.getType()));

            logger.debug(String.format("Read column-stat data:  " +
                "nullmask=0x%X, unique=%d, null=%d, min=%s, max=%s",
                nullMask, c.getNumUniqueValues(), c.getNumNullValues(),
                c.getMinValue(), c.getMaxValue()));

            colStats.add(c);
        }

        return new TableStats(numDataPages, numTuples, avgTupleSize, colStats);
    }
}
