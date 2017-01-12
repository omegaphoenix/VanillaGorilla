package edu.caltech.nanodb.commands;


import edu.caltech.nanodb.server.NanoDBServer;


/**
 * This Command class represents the <tt>EXPLAIN</tt> SQL command, which prints
 * out details of how SQL DML statements will be evaluated.
 */
public class ExplainCommand extends Command {

    /** The command to explain! */
    private QueryCommand cmdToExplain;


    /**
     * Construct an explain command.
     *
     * @param cmdToExplain the command that should be explained.
     */
    public ExplainCommand(QueryCommand cmdToExplain) {
        super(Command.Type.UTILITY);
        this.cmdToExplain = cmdToExplain;
    }


    @Override
    public void execute(NanoDBServer server) throws ExecutionException {
        cmdToExplain.setExplain(true);
        cmdToExplain.execute(server);
    }
}
