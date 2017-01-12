package edu.caltech.nanodb.commands;


import edu.caltech.nanodb.expressions.Expression;
import edu.caltech.nanodb.expressions.ExpressionException;
import edu.caltech.nanodb.server.NanoDBServer;
import edu.caltech.nanodb.server.properties.PropertyRegistry;
import edu.caltech.nanodb.server.properties.ReadOnlyPropertyException;
import edu.caltech.nanodb.server.properties.UnrecognizedPropertyException;


/**
 * Implements the "SET VARIABLE ..." command.
 */
public class SetVariableCommand extends Command {


    private String propertyName;


    private Expression valueExpr;


    public SetVariableCommand(String propertyName, Expression valueExpr) {
        super(Command.Type.UTILITY);

        this.propertyName = propertyName;
        this.valueExpr = valueExpr;
    }


    @Override
    public void execute(NanoDBServer server) throws ExecutionException {

        try {
            PropertyRegistry propReg = server.getPropertyRegistry();
            Object value = valueExpr.evaluate();
            propReg.setPropertyValue(propertyName, value);
            out.printf("Set property \"%s\" to value %s%n", propertyName, value);
        }
        catch (UnrecognizedPropertyException e) {
            throw new ExecutionException(e);
        }
        catch (ReadOnlyPropertyException e) {
            throw new ExecutionException(e);
        }
        catch (ExpressionException e) {
            throw new ExecutionException(e);
        }
    }
}
