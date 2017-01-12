package edu.caltech.nanodb.expressions;


import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;

import edu.caltech.nanodb.relations.Schema;


/**
 * A collection of helpful utilities that can be used for generating,
 * analyzing and manipulating predicates.
 */
public class PredicateUtils {
    /** This class should not be instantiated. */
    private PredicateUtils() {
        throw new RuntimeException("This class should not be instantiated.");
    }

    /**
     * This helper function takes a collection of conjuncts that should comprise
     * a predicate, and creates a predicate for evaluating these conjuncts.  The
     * exact nature of the predicate depends on the conjuncts:
     * <ul>
     *   <li>If the collection contains only one conjunct, the method simply
     *       returns that one conjunct.</li>
     *   <li>If the collection contains two or more conjuncts, the method
     *       returns a {@link BooleanOperator} that performs an <tt>AND</tt> of
     *       all conjuncts.</li>
     *   <li>If the collection contains <em>no</em> conjuncts then the method
     *       returns <tt>null</tt>.
     * </ul>
     *
     * @param conjuncts the collection of conjuncts to combine into a predicate.
     *
     * @return a predicate for evaluating the conjuncts, or <tt>null</tt> if the
     *         input collection contained no conjuncts.
     */
    public static Expression makePredicate(Collection<Expression> conjuncts) {
        Expression predicate = null;
        if (conjuncts.size() == 1) {
            predicate = conjuncts.iterator().next();
        }
        else if (conjuncts.size() > 1) {
            predicate = new BooleanOperator(
                BooleanOperator.Type.AND_EXPR, conjuncts);
        }
        return predicate;
    }


    public static Expression makePredicate(Expression... conjuncts) {
        ArrayList<Expression> list = new ArrayList<Expression>();
        for (Expression conjunct : conjuncts)
            list.add(conjunct);

        return makePredicate(list);
    }


    /**
     * <p>
     * This method takes a predicate <tt>expr</tt> and stores all of its
     * conjuncts into the specified collection of conjuncts.  Specifically, if
     * the predicate is a Boolean <tt>AND</tt> operation then each term will
     * individually be added to the collection of conjuncts.  Any other kind
     * of predicate will be stored as-is into the collection.
     * </p>
     * <p>
     * Note that this method is not clever enough to handle many basic
     * scenarios, such as <tt>P1 AND (P2 AND P3)</tt>, and so forth.
     * </p>
     * <p>
     * Also, note that the input expression is not modified; references to the
     * various conjuncts are simply collected into the specified collection.
     * </p>
     *
     * @param expr the expression to pull the conjuncts out of
     *
     * @param conjuncts the collection of conjuncts to add the predicate (or its
     *        components) to.
     */
    public static void collectConjuncts(Expression expr,
                                        Collection<Expression> conjuncts) {
        // If there is no condition, just return without doing anything.
        if (expr == null)
            return;

        // If it's an AND expression, add the terms to the set of conjuncts.
        if (expr instanceof BooleanOperator) {
            BooleanOperator boolExpr = (BooleanOperator) expr;
            if (boolExpr.getType() == BooleanOperator.Type.AND_EXPR) {
                for (int iTerm = 0; iTerm < boolExpr.getNumTerms(); iTerm++)
                    conjuncts.add(boolExpr.getTerm(iTerm));
            }
            else {
                // The Boolean expression is an OR or NOT, so we can't add the
                // terms themselves.
                conjuncts.add(expr);
            }
        }
        else {
            // The predicate is not a Boolean expression, so just store it.
            conjuncts.add(expr);
        }
    }


    /**
     * <p>
     * This method takes a collection of expressions, and finds those
     * expressions that can be evaluated solely against the provided schemas.
     * In other words, if an expression doesn't refer to any symbols outside
     * of the specified schemas, then it will be included in the result
     * collection.
     * </p>
     * <p>
     * The last argument to this method is a series of {@link Schema} objects
     * to check expressions against.  For example, we can perform operations
     * like this:
     * </p>
     * <pre>
     *   Schema s1 = ...;
     *   Schema s2 = ...;
     *
     *   // Find expressions in srcExprs that can be evaluated against s1, and
     *   // add them to dstExprs.  Do not remove matching exprs from srcExprs.
     *   findExprsUsingSchemas(srcExprs, false, dstExprs, s1);
     *
     *   // Find expressions in srcExprs that can be evaluated against the
     *   // combination of s1 and s2, and add them to dstExprs.  Remove
     *   // matching exprs from srcExprs.
     *   findExprsUsingSchemas(srcExprs, true, dstExprs, s1, s2);
     * </pre>
     *
     * @param srcExprs the input collection of expressions to check against
     *        the provided schemas.
     *
     * @param remove if {@code true}, the matching expressions will be
     *        removed from the {@code srcExprs} collection.  Otherwise, the
     *        {@code srcExprs} collection is left unchanged.
     *
     * @param dstExprs the collection to add the matching expressions to.
     *        This collection is <u>not</u> cleared by this method; any
     *        previous contents in the collection will be left unchanged.
     *
     * @param schemas an array of one or more schemas to check the input
     *        expressions against.  If an expression can be evaluated solely
     *        against these schemas then it will be added to the results.
     */
    public static void findExprsUsingSchemas(Collection<Expression> srcExprs,
        boolean remove, Collection<Expression> dstExprs, Schema... schemas) {

        ArrayList<ColumnName> symbols = new ArrayList<ColumnName>();

        Iterator<Expression> termIter = srcExprs.iterator();
        while (termIter.hasNext()) {
            Expression term = termIter.next();

            // Read all symbols from this term.
            symbols.clear();
            term.getAllSymbols(symbols);

            // If *all* of the symbols in the term reference at least one of
            // the provided schemas, add it to the results (removing from this
            // operator, if so directed by caller).
            boolean allRef = true;
            for (ColumnName colName : symbols) {
                // Determine if *this* symbol references at least one schema.
                boolean ref = false;
                for (Schema schema : schemas) {
                    if (schema.getColumnIndex(colName) != -1) {
                        ref = true;
                        break;
                    }
                }

                // If this symbol doesn't reference any of the schemas then
                // this term doesn't qualify.
                if (!ref) {
                    allRef = false;
                    break;
                }
            }

            if (allRef) {
                dstExprs.add(term);
                if (remove)
                    termIter.remove();
            }
        }
    }
}
