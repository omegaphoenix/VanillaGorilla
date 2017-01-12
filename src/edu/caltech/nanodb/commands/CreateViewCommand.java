package edu.caltech.nanodb.commands;


import edu.caltech.nanodb.queryast.SelectClause;
import edu.caltech.nanodb.server.NanoDBServer;


/**
 * This command-class represents the <tt>CREATE VIEW</tt> DDL command.
 */
public class CreateViewCommand extends Command {

    private String viewName;

    private SelectClause selectClause;


    public CreateViewCommand(String viewName, SelectClause selectClause) {
        super(Type.DDL);

        if (viewName == null)
            throw new IllegalArgumentException("viewName cannot be null");

        if (selectClause == null)
            throw new IllegalArgumentException("selectClause cannot be null");

        this.viewName = viewName;
        this.selectClause = selectClause;
    }


    @Override
    public void execute(NanoDBServer server)
        throws ExecutionException {
        throw new ExecutionException("Not yet implemented!");
    }
}
