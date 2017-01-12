package edu.caltech.nanodb.server;


/**
 * Created by donnie on 10/24/14.
 */
public enum CommandState {
    /**
     * This state value indicates that the server has received the command
     * sent by the client, and is proceeding to execute it.
     */
    COMMAND_RECEIVED,

    /**
     * This state value indicates that the command will be generating a
     * sequence of tuples as the result of a query.
     */
    BEGIN_TUPLES,

    /**
     * This state value indicates that the command has generated all tuples
     * it will generate.
     */
    END_TUPLES,

    /**
     * This state value indicates that the command to be executed has been
     * completed by the server.
     */
    COMMAND_COMPLETED
}
