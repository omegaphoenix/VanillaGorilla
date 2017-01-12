package edu.caltech.nanodb.commands;


import edu.caltech.nanodb.server.NanoDBServer;
import edu.caltech.nanodb.storage.StorageManager;
import edu.caltech.nanodb.transactions.TransactionException;


/**
 * This class represents a command that starts a transaction, such as
 * <tt>BEGIN</tt>, <tt>BEGIN WORK</tt>, or <tt>START TRANSACTION</tt>.
 */
public class BeginTransactionCommand extends Command {
    public BeginTransactionCommand() {
        super(Type.UTILITY);
    }


    @Override
    public void execute(NanoDBServer server) throws ExecutionException {
        // Begin a transaction.
        try {
            // Pass true for the "user-started transaction" flag, since the
            // user issued the command to do it!
            StorageManager storageManager = server.getStorageManager();
            storageManager.getTransactionManager().startTransaction(true);
        }
        catch (TransactionException e) {
            throw new ExecutionException(e);
        }
    }
}
