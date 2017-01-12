package edu.caltech.test.nanodb.sqlparse;


import org.testng.annotations.Test;

import antlr.Token;

import edu.caltech.nanodb.sqlparse.NanoSqlLexer;
import edu.caltech.nanodb.sqlparse.NanoSqlParserTokenTypes;


/**
 * This test-case exercises the SQL lexer for various tokens.
 */
@Test
public class TestLexer extends SqlParseTestCase {

    /** Tests the lexer to ensure that it skips whitespace. **/
    public void testLexWhitespace() throws Exception {
        NanoSqlLexer lexer;
        Token tok;

        // Check skipping a space.
        lexer = getLexerForString(" ");
        tok = lexer.nextToken();
        assert Token.EOF_TYPE == tok.getType() : "Lexer didn't skip space to EOF.";

        // Check skipping a tab.
        lexer = getLexerForString("\t");
        tok = lexer.nextToken();
        assert Token.EOF_TYPE == tok.getType() : "Lexer didn't skip tab to EOF.";

        // Check skipping tabs and spaces.
        lexer = getLexerForString("  \t \t\t \t");
        tok = lexer.nextToken();
        assert Token.EOF_TYPE == tok.getType() : "Lexer didn't skip spaces+tabs to EOF.";
    }


    /**
     * Tests the detection of the symbols '+', '-' and '.' from the lexer rule
     * that matches number literals or symbols.
     */
    public void testLexPlusMinusPeriod() throws Exception {
        checkLexSingleToken("+", NanoSqlParserTokenTypes.PLUS);
        checkLexSingleToken("-", NanoSqlParserTokenTypes.MINUS);
        checkLexSingleToken(".", NanoSqlParserTokenTypes.PERIOD);
    }


    /**
     * Tests the detection of integer (<code>int</code>) literals from the lexer
     * rule that matches number literals or symbols.
     */
    public void testLexIntLiterals() throws Exception {
        checkLexSingleToken("0", NanoSqlParserTokenTypes.INT_LITERAL);
        checkLexSingleToken("5", NanoSqlParserTokenTypes.INT_LITERAL);
        checkLexSingleToken("168671", NanoSqlParserTokenTypes.INT_LITERAL);
        checkLexSingleToken("562135231", NanoSqlParserTokenTypes.INT_LITERAL);
    }


    /**
     * Tests the detection of <code>long</code> literals from the lexer rule
     * that matches number literals or symbols.
     */
    public void testLexLongLiterals() throws Exception {
        // Long literals only accept "L" as the suffix, not "l" (lowercase L).
        checkLexSingleToken("0L", NanoSqlParserTokenTypes.LONG_LITERAL, "0");
        checkLexSingleToken("5L", NanoSqlParserTokenTypes.LONG_LITERAL, "5");
        checkLexSingleToken("168671L", NanoSqlParserTokenTypes.LONG_LITERAL,
            "168671");
        checkLexSingleToken("562135231L", NanoSqlParserTokenTypes.LONG_LITERAL,
            "562135231");
    }


    /**
     * Tests the detection of decimal (<code>double</code>) literals from the
     * lexer rule that matches number literals or symbols.
     */
    public void testLexDecLiterals() throws Exception {
        checkLexSingleToken("0.0", NanoSqlParserTokenTypes.DEC_LITERAL);
        checkLexSingleToken("0.315", NanoSqlParserTokenTypes.DEC_LITERAL);
        checkLexSingleToken("235.41372", NanoSqlParserTokenTypes.DEC_LITERAL);

        checkLexSingleToken("3.1415", NanoSqlParserTokenTypes.DEC_LITERAL);
        checkLexSingleToken("2.1", NanoSqlParserTokenTypes.DEC_LITERAL);

        checkLexSingleToken("0.", NanoSqlParserTokenTypes.DEC_LITERAL);
        checkLexSingleToken("9.", NanoSqlParserTokenTypes.DEC_LITERAL);
        checkLexSingleToken("630.", NanoSqlParserTokenTypes.DEC_LITERAL);

        checkLexSingleToken(".0", NanoSqlParserTokenTypes.DEC_LITERAL);
        checkLexSingleToken(".4", NanoSqlParserTokenTypes.DEC_LITERAL);
        checkLexSingleToken(".315", NanoSqlParserTokenTypes.DEC_LITERAL);
    }


    /**
     * Tests the detection of float literals from the lexer rule
     * that matches number literals or symbols.
     */
    public void testLexFloatLiterals() throws Exception {
        checkLexSingleToken("0.0f", NanoSqlParserTokenTypes.FLOAT_LITERAL, "0.0");
        checkLexSingleToken("0.315F", NanoSqlParserTokenTypes.FLOAT_LITERAL, "0.315");
        checkLexSingleToken("235.41372f", NanoSqlParserTokenTypes.FLOAT_LITERAL, "235.41372");

        checkLexSingleToken("3.1415f", NanoSqlParserTokenTypes.FLOAT_LITERAL, "3.1415");
        checkLexSingleToken("2.1F", NanoSqlParserTokenTypes.FLOAT_LITERAL, "2.1");

        checkLexSingleToken("0.f", NanoSqlParserTokenTypes.FLOAT_LITERAL, "0.");
        checkLexSingleToken("9.F", NanoSqlParserTokenTypes.FLOAT_LITERAL, "9.");
        checkLexSingleToken("630.f", NanoSqlParserTokenTypes.FLOAT_LITERAL, "630.");

        checkLexSingleToken(".0f", NanoSqlParserTokenTypes.FLOAT_LITERAL, ".0");
        checkLexSingleToken(".4F", NanoSqlParserTokenTypes.FLOAT_LITERAL, ".4");
        checkLexSingleToken(".315f", NanoSqlParserTokenTypes.FLOAT_LITERAL, ".315");
    }


    /** Tests the detection of string literals. */
    public void testLexStringLiterals() throws Exception {
        // The single-quotes are removed by the lexer.

        checkLexSingleToken("'hello'", NanoSqlParserTokenTypes.STRING_LITERAL, "hello");
        checkLexSingleToken("'hi there'", NanoSqlParserTokenTypes.STRING_LITERAL, "hi there");
        checkLexSingleToken("'Some! Punctuation, please?!'",
            NanoSqlParserTokenTypes.STRING_LITERAL, "Some! Punctuation, please?!");
        checkLexSingleToken("'\" double \" quotes \"'",
            NanoSqlParserTokenTypes.STRING_LITERAL, "\" double \" quotes \"");
        checkLexSingleToken("''", NanoSqlParserTokenTypes.STRING_LITERAL, "");
    }

    /** Tests the detection of identifiers. */
    public void testLexIdent() throws Exception {
        checkLexSingleToken("a", NanoSqlParserTokenTypes.IDENT, "A");
        checkLexSingleToken("thing", NanoSqlParserTokenTypes.IDENT, "THING");
        checkLexSingleToken("n123", NanoSqlParserTokenTypes.IDENT, "N123");
        checkLexSingleToken("B", NanoSqlParserTokenTypes.IDENT);
        checkLexSingleToken("ITEM", NanoSqlParserTokenTypes.IDENT);
        checkLexSingleToken("TK421", NanoSqlParserTokenTypes.IDENT);
        checkLexSingleToken("UNDER_SCORE_", NanoSqlParserTokenTypes.IDENT);
        checkLexSingleToken("UNDER_SCORE_2", NanoSqlParserTokenTypes.IDENT);
    }
}
