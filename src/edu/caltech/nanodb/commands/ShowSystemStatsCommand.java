package edu.caltech.nanodb.commands;


import edu.caltech.nanodb.server.NanoDBServer;
import edu.caltech.nanodb.server.performance.PerformanceCounters;


/**
 * Implements the "SHOW [system] STATS" command.
 */
public class ShowSystemStatsCommand extends Command {

    public static final String STORAGE_SYSTEM = "storage";


    /** The subsystem that we are displaying statistics for. */
    private String systemName;


    /**
     * These are the performance counters corresponding to various subsystems.
     */
    private static final String[][] PERF_COUNTERS = {
        { STORAGE_SYSTEM, PerformanceCounters.STORAGE_PAGES_READ },
        { STORAGE_SYSTEM, PerformanceCounters.STORAGE_PAGES_WRITTEN },
        { STORAGE_SYSTEM, PerformanceCounters.STORAGE_FILE_CHANGES },
        { STORAGE_SYSTEM, PerformanceCounters.STORAGE_FILE_DISTANCE_TRAVELED }
    };


    public ShowSystemStatsCommand(String systemName) {
        super(Command.Type.UTILITY);

        if (systemName == null)
            throw new IllegalArgumentException("systemName cannot be null");

        this.systemName = systemName.trim().toLowerCase();

        // Make sure the actual system-name is recognized!
        if (!this.systemName.equals(STORAGE_SYSTEM)) {
            throw new IllegalArgumentException(
                "Unrecognized system-stats argument:  " + this.systemName);
        }
    }


    @Override
    public void execute(NanoDBServer server) throws ExecutionException {
        for (String[] pair : PERF_COUNTERS) {
            if (pair[0].equals(systemName)) {
                String name = pair[1];
                long value = PerformanceCounters.get(name);
                out.printf("%s = %d%n", name, value);
            }
        }
    }
}

