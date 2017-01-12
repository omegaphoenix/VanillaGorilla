package edu.caltech.nanodb.server.performance;



import java.util.HashSet;
import java.util.Set;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.log4j.Logger;


/**
 * This class provides a basic performance-counter mechanism that can be used
 * concurrently from many different threads.  It allows us to record
 * performance statistics from the database, and ultimately to expose them
 * through SQL queries as well.
 *
 * @review (Donnie) I really don't like that this is a static class, because
 *         it prevents us from having multiple sets of performance counters.
 *         This is just the easiest way to add counters to NanoDB in the short
 *         run.  In the future, make this non-static, and maybe stick an
 *         instance of it on the NanoDB Server object.  We can put it into
 *         thread-local storage to make it easy to access.
 */
public class PerformanceCounters {
    /** A logging object for reporting anything interesting that happens. */
    private static Logger logger = Logger.getLogger(PerformanceCounters.class);


    public static final String STORAGE_FILE_CHANGES = "storage.fileChanges";


    public static final String STORAGE_FILE_DISTANCE_TRAVELED = "storage.fileDistanceTraveled";


    public static final String STORAGE_PAGES_READ = "storage.pagesRead";


    public static final String STORAGE_PAGES_WRITTEN = "storage.pagesWritten";


    public static final String STORAGE_BYTES_READ = "storage.bytesRead";


    public static final String STORAGE_BYTES_WRITTEN = "storage.bytesWritten";


    private static ConcurrentHashMap<String, AtomicLong> counters =
        new ConcurrentHashMap<>();


    private static AtomicLong getCounter(String counterName) {
        // Do this in two steps so that we can try to avoid allocating an
        // AtomicInteger unless it looks like we need to.
        AtomicLong counter = counters.get(counterName);
        if (counter == null) {
            counters.putIfAbsent(counterName, new AtomicLong());
            counter = counters.get(counterName);
        }

        return counter;
    }


    public static long inc(String counterName) {
        return getCounter(counterName).incrementAndGet();
    }


    public static long add(String counterName, long value) {
        return getCounter(counterName).addAndGet(value);
    }


    public static long dec(String counterName) {
        return getCounter(counterName).decrementAndGet();
    }


    public static long sub(String counterName, long value) {
        return add(counterName, -value);
    }


    public static long get(String counterName) {
        return getCounter(counterName).get();
    }


    public static long clear(String counterName) {
        return getCounter(counterName).getAndSet(0);
    }


    public static void clearAll() {
        counters.clear();
    }


    public static Set<String> getCounterNames() {
        return new HashSet<String>(counters.keySet());
    }
}
