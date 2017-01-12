package edu.caltech.nanodb.expressions;


import java.util.Collection;

import edu.caltech.nanodb.queryast.SelectClause;
import edu.caltech.nanodb.relations.ColumnInfo;
import edu.caltech.nanodb.relations.Schema;
import edu.caltech.nanodb.relations.SchemaNameException;


/**
 * This is the base type of all arithmetic and logical expressions that can be
 * represented in SQL commands.  Expressions can contain literal values,
 * symbolic references (e.g. to columns on tables), arithmetic and logical
 * operators, SQL operators such as <tt>LIKE</tt> and <tt>IN</tt>, and so forth.
 * <p>
 * This class also provides a mechanism for evaluating these expressions, via
 * the {@link #evaluate()} and {@link #evaluate(Environment)} methods.
 * Evaluation may or may not require an "environment" which specifies the
 * current values for symbols that appear within the expression.  The
 * {@link #hasSymbols} method reports whether the expression contains any
 * symbols that would require an environment for evaluation.  If the expression
 * does not contain symbols, it can be evaluated without an environment.
 */
public abstract class Expression implements Cloneable {

    private static class SymbolFinder implements ExpressionProcessor {

        private boolean hasSymbols;

        private Collection<ColumnName> symbols;

        public SymbolFinder(Collection<ColumnName> symbols) {
            hasSymbols = false;
            this.symbols = symbols;
        }

        public SymbolFinder() {
            this(null);
        }

        public void enter(Expression e) {
            if (e instanceof ColumnValue) {
                hasSymbols = true;
                if (symbols != null) {
                    ColumnValue cv = (ColumnValue) e;
                    symbols.add(cv.getColumnName());
                }
            }
            else if (e instanceof SubqueryOperator) {
                SubqueryOperator subOp = (SubqueryOperator) e;
                SelectClause subquery = subOp.getSubquery();
                if (subquery.isCorrelated()) {
                    // Pull out the symbol names that the subquery references
                    // in enclosing queries.
                    hasSymbols = true;
                    symbols.addAll(subquery.getCorrelatedColumns());
                }
            }
        }

        public Expression leave(Expression e) {
            // This function never changes the node that is traversed.
            return e;
        }
    }


    /**
     * Returns a {@link ColumnInfo} object describing the type (and possibly
     * the name) of the expression's result.
     *
     * @param schema a schema object that can be used to look up name and type
     *        details for symbols referenced by the expression.
     *
     * @return a column-information object describing the type (and possibly the
     *         name and table-name) of this expression's result
     *
     * @throws SchemaNameException if a symbol cannot be resolved, either
     *         because it doesn't appear in the schema, or because the name is
     *         ambiguous.
     */
    public abstract ColumnInfo getColumnInfo(Schema schema)
        throws SchemaNameException;


    /**
     * Evaluates this expression object in the context of the specified
     * environment.  The environment provides any external information necessary
     * to evaluate the expression, such as the current tuples loaded from tables
     * referenced within the expression.
     *
     * @param env the environment to look up symbol-values from, when evaluating
     *        the expression
     *
     * @return the result of the expression evaluation
     *
     * @throws ExpressionException if the expression cannot be evaluated for
     *         some reason.
     */
    public abstract Object evaluate(Environment env) throws ExpressionException;


    /**
     * Evaluates this expression object without any environment.  This method
     * may only be used if the expression doesn't contain any symbolic values.
     * If the expression does contain symbols then an exception will be thrown
     * at evaluation time.
     *
     * @return the result of the expression evaluation
     *
     * @throws ExpressionException if the expression cannot be evaluated for
     *         some reason.
     */
    public Object evaluate() throws ExpressionException {
        return evaluate(null);
    }


    /**
     * Evaluates an expression as a Boolean predicate.
     *
     * @param env the environment that the predicate is evaluated within
     *
     * @return the result of the evaluation, converted to a Boolean value
     *
     * @throws ExpressionException if an error occurred during evaluation
     */
    public boolean evaluatePredicate(Environment env) throws ExpressionException {
        Object result = evaluate(env);
        if (result == null)
            return false;   // TODO:  This is UNKNOWN, not FALSE.
        else
            return TypeConverter.getBooleanValue(result);
    }


    /**
     * <p>
     * This method allows the entire expression tree to be traversed node by
     * node, either for analysis or for transformation.  The
     * {@code ExpressionProcessor} instance receives notifications as each
     * node in the expression is entered and left.
     * </p>
     * <p>
     * The expression tree can also be manipulated by this traversal process,
     * depending on what the {@code ExpressionProcessor} wants to do.  If the
     * expression node that {@code traverse()} is invoked on, needs to be
     * replaced with a new expression node, the replacement is returned by the
     * {@code traverse()} method.  (The {@code ExpressionProcessor} specifies
     * the replacement as the return-value from the
     * {@link ExpressionProcessor#leave(Expression)} method.)
     * </p>
     *
     * @param p the object that performs analysis or transformation of the
     *        expression tree
     *
     * @return an {@code Expression} node to replace this node, or
     *         {@code null} if no changes are to be made.
     */
    public abstract Expression traverse(ExpressionProcessor p);


    /**
     * Returns true if this expression contains any symbols that will require an
     * environment to evaluate.  If this method returns false then the expression
     * contains only literals, and therefore can be evaluated without an
     * environment.
     *
     * @return true if the expression contains any symbols, false otherwise
     */
    public final boolean hasSymbols() {
        SymbolFinder f = new SymbolFinder();
        traverse(f);
        return f.hasSymbols;
    }


    /**
     * This method stores all of the symbols in an expression into a collection,
     * so that the expression's symbols can be validated against the schema that
     * the expression will be evaluated against.
     * <p>
     * Note that the specific kind of collection can be varied, to achieve
     * different results.  Callers that need <em>every single
     * symbol-reference</em> can pass in a {@link java.util.List} implementation,
     * while callers that only need to know what symbols are referenced could pass
     * a {@link java.util.Set} implementation as the argument.
     *
     * @param symbols A collection that will receive all of the symbols in the
     *        expression.
     */
    public final void getAllSymbols(Collection<ColumnName> symbols) {
        traverse(new SymbolFinder(symbols));
    }


    /**
     * Returns an <tt>Expression</tt> reference to a (possibly) simplified
     * version of this expression.  If it's not possible to simplify the
     * expression, or if the expression class doesn't support simplification,
     * then the return-value will simply be this expression object, unsimplified.
     *
     * @return a reference to an expression, either a simplified version of this
     *         expression, or the original unmodified expression
     */
    public Expression simplify() {
        return this;
    }


    /**
     * Checks if the argument is an expression with the same structure, but not
     * necesarily the same references.
     *
     * @param obj the object to which we are comparing
     */
    @Override
    public abstract boolean equals(Object obj);


    /**
     * Computes the hashcode of an Expression.  This method is used to see if
     * two expressions CAN be equal.
     */
    @Override
    public abstract int hashCode();


    /**
     * Creates a copy of expression.
     */
    @Override
    protected Object clone() throws CloneNotSupportedException {
        return super.clone();
    }


    /**
     * Returns a deep copy of this expression.
     *
     * @return a deep copy of this expression
     */
    public Expression duplicate() {
        Expression expr;
        try {
            expr = (Expression) clone();
        }
        catch (CloneNotSupportedException e) {
            throw new RuntimeException(e);
        }

        return expr;
    }
}
