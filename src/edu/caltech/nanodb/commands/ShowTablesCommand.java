package edu.caltech.nanodb.commands;


import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;

import edu.caltech.nanodb.server.properties.PropertyRegistry;
import edu.caltech.nanodb.server.properties.UnrecognizedPropertyException;
import edu.caltech.nanodb.storage.StorageManager;
import edu.caltech.nanodb.server.NanoDBServer;

/**
 * Implements the "SHOW TABLES" command.
 */
public class ShowTablesCommand extends Command {

    public ShowTablesCommand() {
        super(Command.Type.UTILITY);
    }


    @Override
    public void execute(NanoDBServer server)
        throws ExecutionException {

        StorageManager storageManager = server.getStorageManager();
        ArrayList<String> tableNames =
            storageManager.getTableManager().getTables();
        Collections.sort(tableNames);

        // Determine max name length. Start at 10 because "TABLE NAME" is 10
        // chars long, and so we'd be at least that wide anyway.
        int maxNameLength = 10;
        for (String name : tableNames) {
            if (name.length() > maxNameLength)
                maxNameLength = name.length();
        }

        // Create formatting
        String formatStr = String.format("| %%-%ds |%%n",
            maxNameLength);
        char[] lines = new char[maxNameLength + 4];
        Arrays.fill(lines, '-');
        lines[0] = '+';
        lines[lines.length - 1] = '+';
        String lineStr = new String(lines);

        // Print out table information with headers
        out.println(lineStr);
        out.printf(formatStr, "TABLE NAME");
        out.println(lineStr);
        for (String name : tableNames)
            out.printf(formatStr, name);

        out.println(lineStr);
    }
}
