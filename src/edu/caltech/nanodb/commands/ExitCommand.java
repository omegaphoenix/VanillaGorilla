package edu.caltech.nanodb.commands;


import edu.caltech.nanodb.server.NanoDBServer;
import edu.caltech.nanodb.storage.StorageManager;


/**
 * This Command class represents the <tt>EXIT</tt> or <tt>QUIT</tt> SQL
 * commands.  These commands aren't standard SQL of course, but are the
 * way that we tell the database to stop.
 */
public class ExitCommand extends Command {

    /** Construct an exit command. */
    public ExitCommand() {
        super(Command.Type.UTILITY);
    }

    /**
     * This method really doesn't do anything, and it isn't intended to be
     * called.  The server looks for this command when commands are being
     * executed, and handles it separately.
     *
     * @review (Donnie) We could actually have this command operate on the
     *         server now that we pass it in...  Need to think about this.
     */
    @Override
    public void execute(NanoDBServer server) throws ExecutionException {
        // Do nothing.
    }
}
