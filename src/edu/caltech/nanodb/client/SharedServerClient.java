package edu.caltech.nanodb.client;


import java.io.EOFException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.PrintStream;
import java.net.Socket;
import java.net.SocketException;
import java.nio.channels.ClosedByInterruptException;
import java.util.concurrent.Semaphore;

import org.apache.log4j.Logger;

import edu.caltech.nanodb.commands.Command;
import edu.caltech.nanodb.queryeval.PrettyTuplePrinter;
import edu.caltech.nanodb.relations.Schema;
import edu.caltech.nanodb.relations.Tuple;
import edu.caltech.nanodb.server.CommandState;
import edu.caltech.nanodb.server.SharedServer;


/**
 * This class implements a client the can connect to the NanoDB
 * {@link edu.caltech.nanodb.server.SharedServer shared server} and
 * send/receive commands and data.
 */
public class SharedServerClient extends InteractiveClient {
    private static Logger logger = Logger.getLogger(SharedServerClient.class);


    /** The socket used to communicate with the shared server. */
    private Socket socket;


    /**
     * This stream is used to receive objects (tuples, messages, etc.) from
     * the server. */
    private ObjectInputStream objectInput;


    /**
     * This stream is used to send objects (commands, specifically) to the
     * server.
     */
    private ObjectOutputStream objectOutput;


    /**
     * This object receives data from the server asynchronously,
     * and prints out whatever it receives.  It is wrapped by the
     * {@link #receiverThread}.
     */
    private Receiver receiver;


    /**
     * This is the thread that the {@link #receiver} object runs within.
     */
    private Thread receiverThread;


    /**
     * This semaphore is used to coordinate when a command has been sent to
     * the server, and when the server is finished sending results back to
     * the client, so that another command cannot be sent until the current
     * one is finished.
     */
    private Semaphore semCommandDone;


    /**
     * This helper class prints out the results that come back from the
     * server.  It is intended to run within a separate thread.
     */
    private class Receiver implements Runnable {
        /** The print-stream to output server results on. */
        private PrintStream out;


        /** A flag indicating when the receiver thread should shut down. */
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
                }
                catch (EOFException e) {
                    System.out.println("Connection was closed by the server.");
                    break;
                }
                catch (SocketException e) {
                    System.out.println("Connection was closed by the server.");
                    break;
                }
                catch (ClosedByInterruptException e) {
                    System.out.println("Thread was interrupted during an IO operation.");
                    break;
                }
                catch (Exception e) {
                    System.out.println("Exception occurred:");
                    e.printStackTrace(System.out);
                }
            }
        }


        public void shutdown() {
            done = true;
        }
    }



    public SharedServerClient(String hostname, int port) throws IOException {
        // Try to establish a connection to the shared database server.
        socket = new Socket(hostname, port);
        objectOutput = new ObjectOutputStream(socket.getOutputStream());
        objectInput = new ObjectInputStream(socket.getInputStream());

        semCommandDone = new Semaphore(0);
    }


    public void startup() {
        // Start up the receiver thread that will print out whatever comes
        // across the wire.
        receiver = new Receiver(System.out);
        receiverThread = new Thread(receiver);
        receiverThread.start();
    }


    public void handleCommand(Command cmd) throws Exception {
        // The typedBytes member will have exactly what the user typed for a
        // command.  We want to send that string, since the Command object
        // itself is too complicated to send across the wire.
        String commandString = typedBytes.toString();
        objectOutput.writeObject(commandString);

        // Wait for the command to be completed.
        semCommandDone.acquire();
    }


    public void shutdown() throws IOException {
        receiver.shutdown();
        receiverThread.interrupt();

        objectInput.close();
        objectOutput.close();
        socket.close();
    }


    /**
     * Entry-point for the shared-server client program.
     *
     * @todo Eventually this program will accept command-line arguments to
     *       specify where the server lives.
     *
     * @param args Currently, no command-line arguments are supported.
     * @throws Exception if an error occurs during interactions with the
     *         server.
     */
    public static void main(String args[]) throws Exception {
        SharedServerClient client = new SharedServerClient("localhost",
            SharedServer.DEFAULT_SERVER_PORT);

        client.startup();
        client.mainloop();
        client.shutdown();
    }
}
