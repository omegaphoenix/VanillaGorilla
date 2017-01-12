package edu.caltech.nanodb.client;


import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;

import org.apache.commons.io.input.TeeInputStream;
import org.apache.log4j.Logger;

import antlr.InputBuffer;
import antlr.LexerSharedInputState;
import antlr.RecognitionException;
import antlr.TokenStreamException;
import edu.caltech.nanodb.commands.Command;
import edu.caltech.nanodb.commands.ExitCommand;
import edu.caltech.nanodb.sqlparse.NanoSqlLexer;
import edu.caltech.nanodb.sqlparse.NanoSqlParser;


/**
 * Created by donnie on 10/24/14.
 */
public abstract class InteractiveClient {

    private static Logger logger = Logger.getLogger(ExclusiveClient.class);


    public static final String LOGGING_CONF_FILE = "logging.conf";


    public static final String CMDPROMPT_FIRST = "CMD> ";


    public static final String CMDPROMPT_NEXT = "   > ";


    /**
     * This class provides a simple wrapper around the NanoSQL Lexer so that the
     * program can present a more user-friendly prompt for multi-line command
     * input.
     */
    protected static class InteractiveLexer extends NanoSqlLexer {

        public InteractiveLexer(InputStream in) { super(in); }
        public InteractiveLexer(Reader in) { super(in); }
        public InteractiveLexer(InputBuffer ib) { super(ib); }
        public InteractiveLexer(LexerSharedInputState state) { super(state); }

        private boolean ignoreNewline = false;

        public void ignoreNextNewline() {
            ignoreNewline = true;
        }

        public void newline() {
            super.newline();

            if (ignoreNewline)
                ignoreNewline = false;
            else
                System.out.print(CMDPROMPT_NEXT);
        }
    }


    protected TeeInputStream tee;


    protected ByteArrayOutputStream typedBytes;


    public abstract void startup() throws Exception;


    public void mainloop() {
        System.out.println("Welcome to NanoDB.  Exit with EXIT or QUIT command.\n");

        // DataInputStream input = new DataInputStream(System.in);
        //NanoSqlLexer lexer = new NanoSqlLexer(input);

        typedBytes = new ByteArrayOutputStream();
        tee = new TeeInputStream(System.in, typedBytes);
        InteractiveLexer lexer = new InteractiveLexer(tee);
        NanoSqlParser parser = new NanoSqlParser(lexer);

        boolean firstCommand = true;
        while (true) {
            try {
                if (firstCommand)
                    firstCommand = false;
                else
                    lexer.ignoreNextNewline();

                typedBytes.reset();
                System.out.print(CMDPROMPT_FIRST);
                Command cmd = parser.command_semicolon();
                logger.debug("Parsed command:  " + cmd);

                if (cmd == null || cmd instanceof ExitCommand)
                    break;

                handleCommand(cmd);
            }
            catch (RecognitionException e) {
                System.out.println("Parser error:  " + e.getMessage());
                logger.error("Parser error", e);
            }
            catch (TokenStreamException e) {
                System.out.println("Input stream error:  " + e.getMessage());
                logger.error("Input stream error", e);
            }
            catch (Exception e) {
                System.out.println("Unexpected error:  " + e.getMessage());
                logger.error("Unexpected error", e);
            }
        }
    }


    public abstract void handleCommand(Command cmd) throws Exception;


    public abstract void shutdown() throws Exception;
}
