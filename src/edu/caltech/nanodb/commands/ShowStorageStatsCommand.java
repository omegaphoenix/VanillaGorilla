package edu.caltech.nanodb.commands;


import edu.caltech.nanodb.server.NanoDBServer;
import edu.caltech.nanodb.server.performance.PerformanceCounters;


/**
 * Implements the "SHOW STORAGE STATS" command.
 */
public class ShowStorageStatsCommand extends Command {

    /**
     * These are the performance counters that we display when someone shows
     * the storage statistics.
     */
    private static final String[] STORAGE_COUNTERS = {
        PerformanceCounters.STORAGE_PAGES_READ,
        PerformanceCounters.STORAGE_PAGES_WRITTEN,
        PerformanceCounters.STORAGE_FILE_CHANGES,
        PerformanceCounters.STORAGE_FILE_DISTANCE_TRAVELED
    };


    public ShowStorageStatsCommand() {
        super(Command.Type.UTILITY);
    }


    @Override
    public void execute(NanoDBServer server) throws ExecutionException {
        for (String name : STORAGE_COUNTERS) {
            long value = PerformanceCounters.get(name);
            out.printf("%s = %d%n", name, value);
        }
    }
}
