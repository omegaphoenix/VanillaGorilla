package edu.caltech.nanodb.server;


import edu.caltech.nanodb.commands.Command;


/**
 * This interface can be implemented by components that need to do processing
 * before and/or after a command is executed.  Command-event listeners need to
 * be registered on the {@link EventDispatcher} before they will be invoked.
 */
public interface CommandEventListener {
    /**
     * This method is called before a command is executed, to allow listeners to
     * perform before-command processing.
     *
     * @param cmd the command about to be executed
     */
    void beforeCommandExecuted(Command cmd) throws EventDispatchException;


    /**
     * This method is called after a command is executed, to allow listeners to
     * perform after-command processing.
     *
     * @param cmd the command that was just executed
     */
    void afterCommandExecuted(Command cmd) throws EventDispatchException;
}
