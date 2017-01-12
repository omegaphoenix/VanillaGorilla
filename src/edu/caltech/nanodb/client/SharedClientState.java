package edu.caltech.nanodb.client;

import java.io.EOFException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.PrintStream;
import java.net.Socket;
import java.net.SocketException;
import java.nio.channels.ClosedByInterruptException;
import java.util.ArrayList;
import java.util.concurrent.Semaphore;

import org.apache.log4j.Logger;

import edu.caltech.nanodb.expressions.TupleLiteral;
import edu.caltech.nanodb.queryeval.PrettyTuplePrinter;
import edu.caltech.nanodb.relations.Schema;
import edu.caltech.nanodb.relations.Tuple;
import edu.caltech.nanodb.server.CommandState;


/**
 * This class represents
 *
 */
public class SharedClientState {
    private static Logger logger = Logger.getLogger(SharedClientState.class);


    private Socket socket;


    private ObjectInputStream objectInput;


    private Receiver receiver;


    /**
     * This thread receives data from the server asynchronously, and prints
     * out whatever it receives.
     */
    private Thread receiverThread;


    private PrintStream out;


    private boolean printTuples;


    private Semaphore semCommandDone;


    private Schema schema;


    private ArrayList<TupleLiteral> tuples;


    /**
     * This stream is used to send objects (commands, specifically) to the
     */
    private ObjectOutputStream objectOutput;


    private class Receiver implements Runnable {

        private PrintStream out;


        private boolean done;


        public Receiver(PrintStream out) {
            this.out = out;
        }


        public void run() {
            PrettyTuplePrinter tuplePrinter = null;

            done = false;
            while (true) {
                try {
                    Object obj = objectInput.readObject();
                    if (obj instanceof String) {
                        // Just print strings to the console
                        System.out.print(obj);
                    }
                    else if (obj instanceof Schema) {
                        tuplePrinter = new PrettyTuplePrinter(out);
                        tuplePrinter.setSchema((Schema) obj);
                    }
                    else if (obj instanceof Tuple) {
                        tuplePrinter.process((Tuple) obj);
                    }
                    else if (obj instanceof Throwable) {
                        Throwable t = (Throwable) obj;
                        t.printStackTrace(System.out);
                    }
                    else if (obj instanceof CommandState) {
                        CommandState state = (CommandState) obj;
                        if (state == CommandState.COMMAND_COMPLETED) {
                            if (tuplePrinter != null) {
                                tuplePrinter.finish();
                                tuplePrinter = null;
                            }

                            // Signal that the command is completed.
                            semCommandDone.release();
                        }
                    }
                    else {
                        // TODO:  Try to print...
                        System.out.println(obj);
                    }
                } catch (EOFException e) {
                    System.out.println("Connection was closed by the server.");
                    break;
                } catch (SocketException e) {
                    System.out.println("Connection was closed by the server.");
                    break;
                } catch (ClosedByInterruptException e) {
                    System.out.println("Thread was interrupted during an IO operation.");
                    break;
                } catch (Exception e) {
                    System.out.println("Exception occurred:");
                    e.printStackTrace(System.out);
                }
            }
        }


        public void shutdown() {
            done = true;
        }
    }


    public SharedClientState(PrintStream out, boolean printTuples) {
        this.out = out;
        this.printTuples = printTuples;
    }


    public void connect(String hostname, int port) throws IOException {
        // Try to establish a connection to the shared database server.
        socket = new Socket(hostname, port);
        objectOutput = new ObjectOutputStream(socket.getOutputStream());
        objectInput = new ObjectInputStream(socket.getInputStream());

        // A semaphore to synchronize the receiver with the code that
        // dispatches commands, so that we don't return from dispatching a
        // command until the server says the command is finished.
        semCommandDone = new Semaphore(0);

        // Start up the receiver thread that will print out whatever comes
        // across the wire.
        receiver = new Receiver(System.out);
        receiverThread = new Thread(receiver);
        receiverThread.start();
    }


    public void doCommand(String commandString) throws Exception {
        schema = null;
        tuples.clear();

        // Send the command to the server!
        objectOutput.writeObject(commandString);

        // Wait for the command to be completed.
        semCommandDone.acquire();
    }


    public void disconnect() throws IOException {
        receiver.shutdown();
        receiverThread.interrupt();

        objectInput.close();
        objectOutput.close();
        socket.close();
    }
}