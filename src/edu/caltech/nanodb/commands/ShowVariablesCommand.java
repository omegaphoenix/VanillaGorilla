package edu.caltech.nanodb.commands;


import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;

import edu.caltech.nanodb.server.NanoDBServer;
import edu.caltech.nanodb.server.properties.PropertyRegistry;
import edu.caltech.nanodb.server.properties.UnrecognizedPropertyException;


/**
 * Implements the "SHOW VARIABLES" command.
 */
public class ShowVariablesCommand extends Command {

    private String filter = null;


    public ShowVariablesCommand() {
        super(Command.Type.UTILITY);
    }


    public void setFilter(String filter) {
        this.filter = filter;
    }


    @Override
    public void execute(NanoDBServer server) throws ExecutionException {

        PropertyRegistry propReg = server.getPropertyRegistry();

        ArrayList<String> propertyNames =
            new ArrayList<>(propReg.getAllPropertyNames());
        Collections.sort(propertyNames);

        ArrayList<String> values = new ArrayList<>();

        int maxNameLength = 0;
        int maxValueLength = 0;

        for (String name : propertyNames) {
            Object value;
            try {
                value = propReg.getPropertyValue(name);
            }
            catch (UnrecognizedPropertyException e) {
                // This will only occur if there is a bug in a component's
                // properties implementation.
                throw new ExecutionException(e);
            }

            String valueStr = (value != null ? value.toString() : null);
            values.add(valueStr);

            if (name.length() > maxNameLength)
                maxNameLength = name.length();

            if (valueStr == null) {
                if (maxValueLength < 4)
                    maxValueLength = 4;
            }
            else if (valueStr.length() > maxValueLength) {
                maxValueLength = valueStr.length();
            }
        }

        String formatStr = String.format("| %%-%ds | %%%ds |%%n",
            maxNameLength, maxValueLength);

        char[] lines = new char[maxNameLength + maxValueLength + 7];
        Arrays.fill(lines, '-');
        lines[0] = '+';
        lines[lines.length - 1] = '+';
        lines[maxNameLength + 3] = '+';
        String lineStr = new String(lines);

        out.println(lineStr);
        out.printf(formatStr, "VARIABLE NAME", "VALUE");
        out.println(lineStr);
        for (int i = 0; i < propertyNames.size(); i++) {
            String name = propertyNames.get(i);
            String valueStr = values.get(i);
            out.printf(formatStr, name, valueStr);
        }
        out.println(lineStr);
    }
}
