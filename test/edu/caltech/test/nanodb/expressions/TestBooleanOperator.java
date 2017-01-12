package edu.caltech.test.nanodb.expressions;


import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.testng.annotations.Test;

import edu.caltech.nanodb.expressions.ArithmeticOperator;
import edu.caltech.nanodb.expressions.BooleanOperator;
import edu.caltech.nanodb.expressions.ColumnName;
import edu.caltech.nanodb.expressions.ColumnValue;
import edu.caltech.nanodb.expressions.Environment;
import edu.caltech.nanodb.expressions.Expression;
import edu.caltech.nanodb.expressions.LiteralValue;


/**
 * This test class exercises the functionality of the {@link BooleanOperator}
 * class.
 */
@Test
public class TestBooleanOperator {

    /**
     * An {@link Expression} class that tracks whether the
     * {@link Expression#evaluate(Environment)} method has been called.  It is
     * used to verify that {@link BooleanOperator} objects lazily evaluate
     * their terms properly.
     */
    private class EvalTracker extends LiteralValue {
        /**
         * This flag reports whether the {@link #evaluate} method has been called.
         */
        public boolean evaluated;

        public EvalTracker(Object value) {
            super(value);
            evaluated = false;
        }

        public Object evaluate(Environment env) {
            evaluated = true;
            return super.evaluate(env);
        }
    }


    /** This method exercises the AND functionality of the Boolean operator. */
    public void testAndOper() {
        BooleanOperator andOp;

        // TRUE && TRUE && TRUE = TRUE

        andOp = new BooleanOperator(BooleanOperator.Type.AND_EXPR);
        andOp.addTerm(new EvalTracker(Boolean.TRUE));
        andOp.addTerm(new EvalTracker(Boolean.TRUE));
        andOp.addTerm(new EvalTracker(Boolean.TRUE));

        assert Boolean.TRUE.equals(andOp.evaluate());
        assert ((EvalTracker) andOp.getTerm(0)).evaluated;
        assert ((EvalTracker) andOp.getTerm(1)).evaluated;
        assert ((EvalTracker) andOp.getTerm(2)).evaluated;

        // TRUE && FALSE && FALSE = FALSE

        andOp = new BooleanOperator(BooleanOperator.Type.AND_EXPR);
        andOp.addTerm(new EvalTracker(Boolean.TRUE));
        andOp.addTerm(new EvalTracker(Boolean.FALSE));
        andOp.addTerm(new EvalTracker(Boolean.FALSE));

        assert Boolean.FALSE.equals(andOp.evaluate());
        assert  ((EvalTracker) andOp.getTerm(0)).evaluated;
        assert  ((EvalTracker) andOp.getTerm(1)).evaluated;
        assert !((EvalTracker) andOp.getTerm(2)).evaluated;

        // FALSE && TRUE && TRUE = FALSE

        andOp = new BooleanOperator(BooleanOperator.Type.AND_EXPR);
        andOp.addTerm(new EvalTracker(Boolean.FALSE));
        andOp.addTerm(new EvalTracker(Boolean.TRUE));
        andOp.addTerm(new EvalTracker(Boolean.TRUE));

        assert Boolean.FALSE.equals(andOp.evaluate());
        assert  ((EvalTracker) andOp.getTerm(0)).evaluated;
        assert !((EvalTracker) andOp.getTerm(1)).evaluated;
        assert !((EvalTracker) andOp.getTerm(2)).evaluated;
    }


    /** This method exercises the OR functionality of the Boolean operator. */
    public void testOrOper() {
        BooleanOperator orOp;

        // FALSE && FALSE && FALSE = FALSE

        orOp = new BooleanOperator(BooleanOperator.Type.OR_EXPR);
        orOp.addTerm(new EvalTracker(Boolean.FALSE));
        orOp.addTerm(new EvalTracker(Boolean.FALSE));
        orOp.addTerm(new EvalTracker(Boolean.FALSE));

        assert Boolean.FALSE.equals(orOp.evaluate());
        assert ((EvalTracker) orOp.getTerm(0)).evaluated;
        assert ((EvalTracker) orOp.getTerm(1)).evaluated;
        assert ((EvalTracker) orOp.getTerm(2)).evaluated;

        // FALSE && TRUE && FALSE = TRUE

        orOp = new BooleanOperator(BooleanOperator.Type.OR_EXPR);
        orOp.addTerm(new EvalTracker(Boolean.FALSE));
        orOp.addTerm(new EvalTracker(Boolean.TRUE));
        orOp.addTerm(new EvalTracker(Boolean.FALSE));

        assert Boolean.TRUE.equals(orOp.evaluate());
        assert  ((EvalTracker) orOp.getTerm(0)).evaluated;
        assert  ((EvalTracker) orOp.getTerm(1)).evaluated;
        assert !((EvalTracker) orOp.getTerm(2)).evaluated;

        // TRUE && FALSE && FALSE = TRUE

        orOp = new BooleanOperator(BooleanOperator.Type.OR_EXPR);
        orOp.addTerm(new EvalTracker(Boolean.TRUE));
        orOp.addTerm(new EvalTracker(Boolean.FALSE));
        orOp.addTerm(new EvalTracker(Boolean.FALSE));

        assert Boolean.TRUE.equals(orOp.evaluate());
        assert  ((EvalTracker) orOp.getTerm(0)).evaluated;
        assert !((EvalTracker) orOp.getTerm(1)).evaluated;
        assert !((EvalTracker) orOp.getTerm(2)).evaluated;
    }


    /** This method exercises the NOT functionality of the Boolean operator. */
    public void testNotOper() {
        BooleanOperator notOp;

        notOp = new BooleanOperator(BooleanOperator.Type.NOT_EXPR);
        notOp.addTerm(new LiteralValue(Boolean.TRUE));
        assert Boolean.FALSE.equals(notOp.evaluate());

        notOp = new BooleanOperator(BooleanOperator.Type.NOT_EXPR);
        notOp.addTerm(new LiteralValue(Boolean.FALSE));
        assert Boolean.TRUE.equals(notOp.evaluate());
    }


    /**
     * This test exercises the {@link BooleanOperator#hasTermsReferencingAllTables}
     * method with various scenarios.
     */
    public void testHasTerms() {
        BooleanOperator boolOp;

        ArrayList<Expression> terms = new ArrayList<Expression>();

        terms.add(new ColumnValue(new ColumnName("t1", "a")));
        terms.add(new ColumnValue(new ColumnName("t2", "a")));
        terms.add(new ColumnValue(new ColumnName("t1", "b")));
        terms.add(new ColumnValue(new ColumnName("t3", "c")));

        terms.add(new ArithmeticOperator(ArithmeticOperator.Type.ADD,
            terms.get(0), terms.get(2)));
        terms.add(new ArithmeticOperator(ArithmeticOperator.Type.ADD,
            terms.get(0), terms.get(3)));
    
        terms.add(new ArithmeticOperator(ArithmeticOperator.Type.ADD,
            terms.get(1), terms.get(5)));

        boolOp = new BooleanOperator(BooleanOperator.Type.AND_EXPR);
        for (Expression term : terms)
            boolOp.addTerm(term);

        assert  boolOp.hasTermsReferencingAllTables("t1");
        assert  boolOp.hasTermsReferencingAllTables("t2");
        assert  boolOp.hasTermsReferencingAllTables("t1", "t3");
        assert !boolOp.hasTermsReferencingAllTables("t4");
        assert !boolOp.hasTermsReferencingAllTables("t1", "t3", "t4");
        assert  boolOp.hasTermsReferencingAllTables("t1", "t2", "t3");
    }


    /**
     * This test exercises the {@link BooleanOperator#getTermsReferencingAllTables}
     * method with various scenarios, some of which return no results, others
     * which return one or more results.
     */
    public void testGetTerms() {
        BooleanOperator boolOp;

        ArrayList<Expression> terms = new ArrayList<Expression>();

        terms.add(new ColumnValue(new ColumnName("t1", "a")));  // Index 0
        terms.add(new ColumnValue(new ColumnName("t2", "a")));  // Index 1
        terms.add(new ColumnValue(new ColumnName("t1", "b")));  // Index 2
        terms.add(new ColumnValue(new ColumnName("t3", "c")));  // Index 3

        // Index 4:  t1 + t1
        terms.add(new ArithmeticOperator(ArithmeticOperator.Type.ADD,
            terms.get(0), terms.get(2)));

        // Index 5:  t1 + t3
        terms.add(new ArithmeticOperator(ArithmeticOperator.Type.ADD,
            terms.get(0), terms.get(3)));

        // Index 6:  t2 + (t1 + t3)
        terms.add(new ArithmeticOperator(ArithmeticOperator.Type.ADD,
            terms.get(1), terms.get(5)));

        boolOp = new BooleanOperator(BooleanOperator.Type.AND_EXPR);
        for (Expression term : terms)
            boolOp.addTerm(term);

        List<Expression> result;

        result = boolOp.getTermsReferencingAllTables("t1");
        verifyFoundTerms(terms, result, new int[] {0, 2, 4});

        result = boolOp.getTermsReferencingAllTables("t2");
        verifyFoundTerms(terms, result, new int[] {1});

        result = boolOp.getTermsReferencingAllTables("t1", "t3");
        verifyFoundTerms(terms, result, new int[] {5});

        result = boolOp.getTermsReferencingAllTables("t4");
        verifyFoundTerms(terms, result, new int[] {});

        result = boolOp.getTermsReferencingAllTables("t1", "t3", "t4");
        verifyFoundTerms(terms, result, new int[] {});

        result = boolOp.getTermsReferencingAllTables("t1", "t2", "t3");
        verifyFoundTerms(terms, result, new int[] {6});
    }


    /**
     * A helper method to scan through terms found by the
     * {@link BooleanOperator#getTermsReferencingAllTables} method,
     * to verify them against an expected set of results.
     */
    private void verifyFoundTerms(List<Expression> terms,
        List<Expression> results, int[] expected) {

        Iterator<Expression> iter = results.iterator();
        for (int i = 0; i < expected.length; i++) {
            assert iter.hasNext() : "Result doesn't contain term " +
                terms.get(expected[i]);

            Expression resultTerm = iter.next();

            assert terms.get(expected[i]) == resultTerm : "Result term " + i +
                " (" + resultTerm + ") isn't expected term " + terms.get(expected[i]);
        }

        assert !iter.hasNext() : "Result contains more terms than expected";
    }
}
