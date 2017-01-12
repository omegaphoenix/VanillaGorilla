package edu.caltech.nanodb.server;


import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;

import org.apache.log4j.Logger;


/**
 * This class implements a "shared" database server that listens for incoming
 * connections on a socket, so that the database can have multiple concurrent
 * clients connected at the same time.
 */
public class SharedServer {
    /** A logging object for reporting anything interesting that happens. */
    private static Logger logger = Logger.getLogger(SharedServer.class);


    /** The default server port is 12200, since we use NanoDB in CS122! */
    public static final int DEFAULT_SERVER_PORT = 12200;


    /** The actual server port being used. */
    private int serverPort = DEFAULT_SERVER_PORT;


    /**
     * The actual NanoDB server that handles incoming requests from
     * various clients.
     */
    private NanoDBServer server = null;


    /**
     * A mapping from client ID to the thread handling the client.  This is
     * declared final since we synchronize on it.
     */
    private final HashMap<Integer, Thread> clientThreads = new HashMap<>();


    public void startup() throws IOException {
        if (server != null)
            throw new IllegalStateException("Server is already started!");

        logger.info("Starting shared database server.");
        server = new NanoDBServer();
        server.startup();

        // Register a shutdown hook so we can shut down the database cleanly.
        Runtime rt = Runtime.getRuntime();
        rt.addShutdownHook(new Thread(new Runnable() {
            public void run() { shutdown(); }
        } ));

        // Start up the server-socket that we receive incoming connections on.
        ServerSocket serverSocket = new ServerSocket(serverPort);
        logger.info("Listening on socket " + serverPort + ".");

        // Wait for a client to connect.  When one does, spin off a thread to
        // handle requests from that client.
        int clientID = 1;
        while (true) {
            logger.info("Waiting for client connection.");
            Socket sock = serverSocket.accept();
            logger.info("Received client connection.");
            ClientHandler clientHandler =
                new ClientHandler(server, clientID, sock);
            Thread t = new Thread(clientHandler);

            // Record the thread so that when the server is being shut down,
            // we can stop all the client threads.
            synchronized (clientThreads) {
                clientThreads.put(clientID, t);
            }

            t.start();
        }
    }


    public void shutdown() {
        for (Thread t : clientThreads.values()) {
            // TODO:  Shut down the client thread.
        }

        server.shutdown();
    }


    public static void main(String[] args) {
        SharedServer server = new SharedServer();
        try {
            server.startup();
        }
        catch (IOException e) {
            System.out.println("Couldn't start shared server:  " +
                e.getMessage());
            e.printStackTrace(System.out);
        }
    }
}
