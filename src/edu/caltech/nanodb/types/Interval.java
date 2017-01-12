package edu.caltech.nanodb.types;


import java.util.Date;
import java.util.GregorianCalendar;


public class Interval implements Comparable<Interval> {

    /** The interval's actual length is given in milliseconds. **/
    private long milliseconds;


    public Interval(long millis) {
        milliseconds = millis;
    }

    public Interval() {
        this(0);
    }

    public Interval(GregorianCalendar fromCal, GregorianCalendar toCal) {
        set(fromCal, toCal);
    }

    public Interval(Date fromDate, Date toDate) {
        set(fromDate, toDate);
    }


    public boolean equals(Object obj) {
        if (obj instanceof Interval) {
            Interval i = (Interval) obj;
            return milliseconds == i.milliseconds;
        }
        return false;
    }


    public int hashCode() {
        // Uses the same hash-code implementation as java.lang.Long.
        return (int) (milliseconds ^ (milliseconds >>> 32));
    }

    public int compareTo(Interval i) {
        long comp = milliseconds - i.milliseconds;

        // Do this comparison stuff just in case the long is larger than
        // what would fit in an int.
        if (comp < 0)
            comp = -1;
        else if (comp > 0)
            comp = 1;

        return (int) comp;
    }

    public void set(GregorianCalendar fromCal, GregorianCalendar toCal) {
        setMillis(toCal.getTimeInMillis() - fromCal.getTimeInMillis());
    }

    public void set(Date fromDate, Date toDate) {
        setMillis(toDate.getTime() - fromDate.getTime());
    }

    public void setMillis(long millis) {
        milliseconds = millis;
    }
}

