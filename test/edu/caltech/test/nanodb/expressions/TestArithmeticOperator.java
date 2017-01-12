package edu.caltech.test.nanodb.expressions;


import org.testng.annotations.Test;

import edu.caltech.nanodb.expressions.ArithmeticOperator;
import edu.caltech.nanodb.expressions.ArithmeticOperator.Type;
import edu.caltech.nanodb.expressions.LiteralValue;


/**
 * This test class exercises the functionality of the {@link ArithmeticOperator}
 * class.
 */
@Test
public class TestArithmeticOperator {

    private static class TestOperation {
        public Type op;

        public Object arg1;

        public Object arg2;

        public Object result;

        public TestOperation(ArithmeticOperator.Type op,
                             Object arg1, Object arg2, Object result) {
            this.op = op;
            this.arg1 = arg1;
            this.arg2 = arg2;
            this.result = result;
        }
    }


    private static TestOperation[] ADD_TESTS = {
        // Simple addition, no casting
        new TestOperation(Type.ADD, new Integer(3), new Integer(4), new Integer(7)),
        new TestOperation(Type.ADD, new Float(2.5), new Float(3.25), new Float(5.75)),
        new TestOperation(Type.ADD, new Long(14), new Long(38), new Long(52)),
        new TestOperation(Type.ADD, new Double(-3.5), new Double(9.0), new Double(5.5)),

        // Addition with casting

        new TestOperation(Type.ADD, new Integer(3), new Float(4.5), new Float(7.5)),
        new TestOperation(Type.ADD, new Float(2.5), new Integer(4), new Float(6.5)),

        new TestOperation(Type.ADD, new Long(3), new Float(4.5), new Float(7.5)),
        new TestOperation(Type.ADD, new Float(2.5), new Long(4), new Float(6.5)),

        new TestOperation(Type.ADD, new Integer(3), new Long(15), new Long(18)),
        new TestOperation(Type.ADD, new Long(12), new Integer(4), new Long(16)),

        new TestOperation(Type.ADD, new Integer(3), new Double(4.5), new Double(7.5)),
        new TestOperation(Type.ADD, new Double(2.5), new Integer(4), new Double(6.5)),

        new TestOperation(Type.ADD, new Long(3), new Double(4.5), new Double(7.5)),
        new TestOperation(Type.ADD, new Double(2.5), new Long(4), new Double(6.5)),

        new TestOperation(Type.ADD, new Double(3.5), new Float(4.75), new Double(8.25)),
        new TestOperation(Type.ADD, new Float(2.5), new Double(1.25), new Double(3.75))
    };


    private static TestOperation[] SUB_TESTS = {
        // Simple subtraction, no casting
        new TestOperation(Type.SUBTRACT, new Integer(3), new Integer(4), new Integer(-1)),
        new TestOperation(Type.SUBTRACT, new Float(2.5), new Float(3.25), new Float(-0.75)),
        new TestOperation(Type.SUBTRACT, new Long(14), new Long(38), new Long(-24)),
        new TestOperation(Type.SUBTRACT, new Double(-3.5), new Double(9.0), new Double(-12.5)),

        // Subtraction with casting

        new TestOperation(Type.SUBTRACT, new Integer(3), new Float(4.5), new Float(-1.5)),
        new TestOperation(Type.SUBTRACT, new Float(2.5), new Integer(4), new Float(-1.5)),

        new TestOperation(Type.SUBTRACT, new Long(3), new Float(4.5), new Float(-1.5)),
        new TestOperation(Type.SUBTRACT, new Float(2.5), new Long(4), new Float(-1.5)),

        new TestOperation(Type.SUBTRACT, new Integer(3), new Long(15), new Long(-12)),
        new TestOperation(Type.SUBTRACT, new Long(12), new Integer(4), new Long(8)),

        new TestOperation(Type.SUBTRACT, new Integer(3), new Double(4.5), new Double(-1.5)),
        new TestOperation(Type.SUBTRACT, new Double(2.5), new Integer(4), new Double(-1.5)),

        new TestOperation(Type.SUBTRACT, new Long(3), new Double(4.5), new Double(-1.5)),
        new TestOperation(Type.SUBTRACT, new Double(2.5), new Long(4), new Double(-1.5)),

        new TestOperation(Type.SUBTRACT, new Double(3.5), new Float(4.75), new Double(-1.25)),
        new TestOperation(Type.SUBTRACT, new Float(2.5), new Double(1.25), new Double(1.25))
    };


    private static TestOperation[] MUL_TESTS = {
        // Simple subtraction, no casting
        new TestOperation(Type.MULTIPLY, new Integer(3), new Integer(4), new Integer(12)),
        new TestOperation(Type.MULTIPLY, new Float(2.5), new Float(3.25), new Float(8.125)),
        new TestOperation(Type.MULTIPLY, new Long(14), new Long(38), new Long(532)),
        new TestOperation(Type.MULTIPLY, new Double(-3.5), new Double(9.0), new Double(-31.5)),

        // Subtraction with casting

        new TestOperation(Type.MULTIPLY, new Integer(3), new Float(4.5), new Float(13.5)),
        new TestOperation(Type.MULTIPLY, new Float(2.5), new Integer(4), new Float(10)),

        new TestOperation(Type.MULTIPLY, new Long(3), new Float(4.5), new Float(13.5)),
        new TestOperation(Type.MULTIPLY, new Float(2.5), new Long(4), new Float(10)),

        new TestOperation(Type.MULTIPLY, new Integer(3), new Long(15), new Long(45)),
        new TestOperation(Type.MULTIPLY, new Long(12), new Integer(4), new Long(48)),

        new TestOperation(Type.MULTIPLY, new Integer(3), new Double(4.5), new Double(13.5)),
        new TestOperation(Type.MULTIPLY, new Double(2.5), new Integer(4), new Double(10)),

        new TestOperation(Type.MULTIPLY, new Long(3), new Double(4.5), new Double(13.5)),
        new TestOperation(Type.MULTIPLY, new Double(2.5), new Long(4), new Double(10)),

        new TestOperation(Type.MULTIPLY, new Double(3.5), new Float(4.75), new Double(16.625)),
        new TestOperation(Type.MULTIPLY, new Float(2.5), new Double(1.25), new Double(3.125))
    };


    /* TODO:  Division probably should generate floating-point results on integer arguments.
    private static TestOperation[] DIV_TESTS = {
        // Simple subtraction, no casting
        new TestOperation(Type.DIVIDE, new Integer(3), new Integer(4), new Integer(12)),
        new TestOperation(Type.DIVIDE, new Float(2.5), new Float(3.25), new Float(8.125)),
        new TestOperation(Type.DIVIDE, new Long(14), new Long(38), new Long(532)),
        new TestOperation(Type.DIVIDE, new Double(-3.5), new Double(9.0), new Double(-31.5)),

        // Subtraction with casting

        new TestOperation(Type.DIVIDE, new Integer(3), new Float(4.5), new Float(13.5)),
        new TestOperation(Type.DIVIDE, new Float(2.5), new Integer(4), new Float(10)),

        new TestOperation(Type.DIVIDE, new Long(3), new Float(4.5), new Float(13.5)),
        new TestOperation(Type.DIVIDE, new Float(2.5), new Long(4), new Float(10)),

        new TestOperation(Type.DIVIDE, new Integer(3), new Long(15), new Long(45)),
        new TestOperation(Type.DIVIDE, new Long(12), new Integer(4), new Long(48)),

        new TestOperation(Type.DIVIDE, new Integer(3), new Double(4.5), new Double(13.5)),
        new TestOperation(Type.DIVIDE, new Double(2.5), new Integer(4), new Double(10)),

        new TestOperation(Type.DIVIDE, new Long(3), new Double(4.5), new Double(13.5)),
        new TestOperation(Type.DIVIDE, new Double(2.5), new Long(4), new Double(10)),

        new TestOperation(Type.DIVIDE, new Double(3.5), new Float(4.75), new Double(16.625)),
        new TestOperation(Type.DIVIDE, new Float(2.5), new Double(1.25), new Double(3.125))
    };
    */


    public void testAdd() {
        runTests(ADD_TESTS);
    }


    public void testSubtract() {
        runTests(SUB_TESTS);
    }


    public void testMultiply() {
        runTests(MUL_TESTS);
    }


    private void runTests(TestOperation[] tests) {
        for (TestOperation test : tests) {
            ArithmeticOperator op = prepareTest(test);
            Object actual = op.evaluate();
            checkTestResult(test, actual);
        }
    }


    private ArithmeticOperator prepareTest(TestOperation test) {
        return new ArithmeticOperator(test.op, new LiteralValue(test.arg1),
            new LiteralValue(test.arg2));
    }

    private void checkTestResult(TestOperation test, Object actual) {
        assert actual.equals(test.result) : "Actual result " + actual +
            " doesn't match expected value " + test.result;
    }
}
