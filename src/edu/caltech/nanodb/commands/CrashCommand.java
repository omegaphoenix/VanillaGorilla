package edu.caltech.nanodb.commands;


import edu.caltech.nanodb.server.NanoDBServer;


/**
 * This command "crashes" the database by shutting it down immediately without
 * any proper cleanup or flushing of caches.
 */
public class CrashCommand extends Command {
    private int secondsToCrash;


    /**
     * Construct a new <tt>CRASH</tt> command that will wait for the specified
     * number of seconds and then crash the database.
     */
    public CrashCommand(int secs) {
        super(Command.Type.UTILITY);

        secondsToCrash = secs;
    }


    /**
     * Construct a new <tt>CRASH</tt> command that will crash the database
     * immediately.
     */
    public CrashCommand() {
        this(0);
    }


    @Override
    public void execute(NanoDBServer server) throws ExecutionException {

        if (secondsToCrash <= 0) {
            // Crash immediately.
            doCrash();
        }
        else {
            // Wait for the specified amount of time in a thread, then crash.
            Thread t = new Thread(new Runnable() {
                public void run() {
                    try {
                        Thread.sleep(secondsToCrash * 1000);
                        out.println("\n");
                        doCrash();
                    } catch (InterruptedException e) {
                        out.println("Crash-thread was interrupted, not crashing.");
                    }
                }
            });
            t.start();
        }
    }


    private void doCrash() {
        out.println("Goodbye, cruel world!  I'm taking your data with me!!!");

        // Using this API call avoids running shutdown hooks, finalizers, etc.
        // 22 is the exit status of the VM's process
        Runtime.getRuntime().halt(22);
    }


    /**
     * Prints a simple representation of the crash command.
     *
     * @return a string representing this crash command
     */
    @Override
    public String toString() {
        String s = "Crash";
        if (secondsToCrash > 0)
            s += "[wait " + secondsToCrash + " seconds]";

        return s;
    }
}
