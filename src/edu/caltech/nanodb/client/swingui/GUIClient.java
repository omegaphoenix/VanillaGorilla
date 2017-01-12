package edu.caltech.nanodb.client.swingui;


import edu.caltech.nanodb.expressions.TupleLiteral;
import edu.caltech.nanodb.relations.Schema;

import java.awt.BorderLayout;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.PrintStream;
import java.net.Socket;

import javax.swing.JButton;
import javax.swing.JFrame;
import javax.swing.JOptionPane;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JSplitPane;
import javax.swing.JTabbedPane;
import javax.swing.JTable;
import javax.swing.JTextArea;
import javax.swing.SwingUtilities;


/**
 * 
 */
public class GUIClient {
    private Socket sock;

    
    private ObjectInputStream objectInput;


    private ObjectOutputStream objectOutput;


    private JFrame frame;

    
    private JSplitPane clientSplitPane;
    
    
    private JTextArea commandInput;
    private JScrollPane commandScroll;
    
    
    private JButton btnExecute;


    private JTabbedPane outputTabbedPane;


    private TupleTableModel tupleModel;

    private JTable tupleOutput;
    private JScrollPane tupleOutputScroll = new JScrollPane(tupleOutput);


    private JTextArea textOutput;
    private JScrollPane textOutputScroll;

    
    public GUIClient(Socket sock) throws IOException {
        this.sock = sock;
        objectOutput = new ObjectOutputStream(sock.getOutputStream());
        objectInput = new ObjectInputStream(sock.getInputStream());
    }
    
    
    public void start() {
        initGUI();
        
        Thread t = new Thread(new DBResponseHandler());
        t.start();
    }
    
    
    private class ActionHandler implements ActionListener {
        @Override
        public void actionPerformed(ActionEvent evt) {
            if (evt.getSource() == btnExecute) {
                String commands = commandInput.getText();
                issueCommands(commands);
            }
        }
    }
    
    
    private class DBResponseHandler implements Runnable {
        public void run() {
            try {
                while (true) {
                    Object obj = objectInput.readObject();
                    if (obj instanceof String) {
                        final String s = (String) obj;
                        SwingUtilities.invokeAndWait(new Runnable() {
                            @Override public void run() {
                                outputTabbedPane.setSelectedIndex(1);
                                textOutput.append(s);
                                textOutput.setCaretPosition(textOutput.getText().length());
                            }
                        });
                    }
                    else if (obj instanceof Schema) {
                        final Schema s = (Schema) obj;
                        SwingUtilities.invokeAndWait(new Runnable() {
                            @Override public void run() {
                                outputTabbedPane.setSelectedIndex(0);
                                tupleModel.setSchema(s);
                            }
                        });
                    }
                    else if (obj instanceof TupleLiteral) {
                        final TupleLiteral t = (TupleLiteral) obj;
                        SwingUtilities.invokeAndWait(new Runnable() {
                            @Override
                            public void run() { tupleModel.addTuple(t); }
                        });
                    }
                    else if (obj instanceof Throwable) {
                        Throwable t = (Throwable) obj;

                        ByteArrayOutputStream baos = new ByteArrayOutputStream();
                        PrintStream ps = new PrintStream(baos);
                        t.printStackTrace(ps);
                        ps.flush();
                        
                        final String msg = "ERROR:  " + baos.toString("US-ASCII");
                        
                        SwingUtilities.invokeAndWait(new Runnable() {
                            @Override public void run() { textOutput.append(msg); }
                        });
                    }
                    else {
                        JOptionPane.showMessageDialog(frame, String.format(
                            "Received unexpected object type!%n%n" +
                            "Class:  %s%n%nValue:  %s", obj.getClass(), obj),
                            "NanoDB Client Error", JOptionPane.WARNING_MESSAGE);
                    }
                }
            }
            catch (Exception e) {
                JOptionPane.showMessageDialog(frame,
                    "Couldn't receive database response:\n\n" + e.getMessage(),
                    "NanoDB Client Error", JOptionPane.ERROR_MESSAGE);
            }
        }
    }
    
    
    private void issueCommands(String commands) {
        commands = commands.trim();
        if (!commands.endsWith(";"))
            commands = commands + ";";

        try {
            objectOutput.writeObject(commands);
        }
        catch (Exception e) {
            JOptionPane.showMessageDialog(frame,
                "Couldn't issue commands:\n\n" + e.getMessage(),
                "NanoDB Client Error", JOptionPane.ERROR_MESSAGE);
        }
    }


    private void initGUI() {
        frame = new JFrame("NanoDB Client");
        frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);

        // Construct the component where the commands will be entered.

        commandInput = new JTextArea();
        commandScroll = new JScrollPane(commandInput);

        btnExecute = new JButton("Execute");
        btnExecute.addActionListener(new ActionHandler());
        
        JPanel cmdPanel = new JPanel(new BorderLayout());
        cmdPanel.add(commandScroll, BorderLayout.CENTER);
        cmdPanel.add(btnExecute, BorderLayout.EAST);

        // Construct the components where the output will be going.

        tupleModel = new TupleTableModel();
        tupleOutput = new JTable(tupleModel);
        tupleOutputScroll = new JScrollPane(tupleOutput);
        
        textOutput = new JTextArea();
        textOutput.setEditable(false);
        textOutputScroll = new JScrollPane(textOutput);

        outputTabbedPane = new JTabbedPane(JTabbedPane.TOP);
        outputTabbedPane.add("Tuples", tupleOutputScroll);
        outputTabbedPane.add("Output", textOutputScroll);
        
        clientSplitPane = new JSplitPane(JSplitPane.VERTICAL_SPLIT,
            cmdPanel, outputTabbedPane);
        
        frame.add(clientSplitPane);
        frame.pack();
        frame.setVisible(true);
    }


    public static void main(String[] args) {
        String hostname = "localhost";
        int port = 12200;
        
        try {
            Socket clientSock = new Socket(hostname, port);
            GUIClient client = new GUIClient(clientSock);
            client.start();
        }
        catch (IOException e) {
            System.out.printf("Couldn't connect to server %s:%d%n", hostname,
                port);
            e.printStackTrace(System.out);
        }
    }
}
