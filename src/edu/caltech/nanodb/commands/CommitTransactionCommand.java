package edu.caltech.nanodb.commands;


import edu.caltech.nanodb.server.NanoDBServer;
import edu.caltech.nanodb.storage.StorageManager;
import edu.caltech.nanodb.transactions.TransactionException;


/**
 * This class represents a command that commits a transaction, such as
 * <tt>COMMIT</tt> or <tt>COMMIT WORK</tt>.
 */
public class CommitTransactionCommand extends Command {
    public CommitTransactionCommand() {
        super(Type.UTILITY);
    }


    @Override
    public void execute(NanoDBServer server) throws ExecutionException {
        // Commit the transaction.
        try {
            StorageManager storageManager = server.getStorageManager();
            storageManager.getTransactionManager().commitTransaction();
        }
        catch (TransactionException e) {
            throw new ExecutionException(e);
        }
    }
}
