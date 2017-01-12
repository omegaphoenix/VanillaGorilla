package edu.caltech.nanodb.types;


public class Time implements Comparable<Time> {

    private static final long MILLIS_IN_DAY = 24 * 60 * 60 * 1000;

    private int hour;

    private int minute;

    private int second;


    private int millisecond;


    public Time(int hour, int minute, int second, int millisecond) {
        this.hour = hour;
        this.minute = minute;
        this.second = second;
        this.millisecond = millisecond;
    }

    public Time(int hour, int minute, int second) {
        this(hour, minute, second, 0);
    }

    public Time(long millisSinceEpoch) {
        int timePart = (int) (millisSinceEpoch % MILLIS_IN_DAY);
        if (timePart < 0)
            timePart += MILLIS_IN_DAY;

        millisecond = timePart % 1000;
        timePart /= 1000;

        second = timePart % 60;
        timePart /= 60;

        minute = timePart % 60;
        timePart /= 60;

        hour = timePart;
        assert(hour < 24);
    }

    public Time() {
        this(System.currentTimeMillis());
    }


    public boolean equals(Object obj) {
        if (obj instanceof Time) {
            Time t = (Time) obj;

            return (hour == t.hour && minute == t.minute &&
                    second == t.second && millisecond == t.millisecond);
        }
        return false;
    }


    public int hashCode() {
        int hash = 17;

        hash = 37 * hash + hour;
        hash = 37 * hash + minute;
        hash = 37 * hash + second;
        hash = 37 * hash + millisecond;

        return hash;
    }


    public int compareTo(Time t) {
        int comp;

        comp = hour - t.hour;
        if (comp == 0) {
            comp = minute - t.minute;
            if (comp == 0) {
                comp = second - t.second;
                if (comp == 0)
                    comp = millisecond - t.millisecond;
            }
        }

        return comp;
    }
}

