package edu.caltech.nanodb.commands;


import java.util.LinkedHashSet;

import edu.caltech.nanodb.server.NanoDBServer;


/**
 * This Command class represents the <tt>OPTIMIZE</tt> SQL command, which
 * optimizes a table's representation (along with any indexes) to improve access
 * performance and space utilization.  This is not a standard SQL command.
 */
public class OptimizeCommand extends Command {

    /**
     * Table names are kept in a set so that we don't need to worry about a
     * particular table being specified multiple times.
     */
    private LinkedHashSet<String> tableNames;


    /**
     * Construct a new <tt>OPTIMIZE</tt> command with an empty table list.
     * Tables can be added to the internal list using the {@link #addTable}
     * method.
     */
    public OptimizeCommand() {
        super(Command.Type.UTILITY);
        tableNames = new LinkedHashSet<>();
    }


    /**
     * Construct a new <tt>OPTIMIZE</tt> command to optimize the specified
     * table.
     *
     * @param tableName the name of the table to optimize.
     */
    public OptimizeCommand(String tableName) {
        this();
        addTable(tableName);
    }


    /**
     * Add a table to the list of tables to optimize.
     *
     * @param tableName the name of the table to optimize.
     */
    public void addTable(String tableName) {
        if (tableName == null)
            throw new NullPointerException("tableName cannot be null");

        tableNames.add(tableName);
    }


    @Override
    public void execute(NanoDBServer server) throws ExecutionException {
        throw new ExecutionException("Not yet implemented!");
    }


    /**
     * Prints a simple representation of the optimize command, including the
     * names of the tables to be optimized.
     *
     * @return a string representing this optimize command
     */
    @Override
    public String toString() {
        return "Optimize[" + tableNames + "]";
    }
}
