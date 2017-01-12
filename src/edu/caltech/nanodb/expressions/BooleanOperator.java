package edu.caltech.nanodb.expressions;


import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;

import edu.caltech.nanodb.relations.ColumnInfo;
import edu.caltech.nanodb.relations.ColumnType;
import edu.caltech.nanodb.relations.Schema;
import edu.caltech.nanodb.relations.SchemaNameException;


/**
 * This class provides the standard Boolean logical operators AND, OR, and NOT,
 * for two or more terms (or exactly one term, in the case of NOT).  In
 * addition, there are several methods provided for manipulating Boolean
 * expressions, since many plan equivalence-rules involve manipulations of the
 * Boolean logical expressions associated with Select and Theta-Join plan nodes.
 */
public class BooleanOperator extends Expression {

    /**
     * This enumeration specifies the different kinds of Boolean operators that
     * this class can implement.
     */
    public enum Type {
        AND_EXPR("AND"),
        OR_EXPR("OR"),
        NOT_EXPR("NOT");

        /** The string representation for each operator.  Used for printing. **/
        private final String stringRep;

        /**
         * Construct a Type enum with the specified string representation.
         *
         * @param rep the string representation of the type
         */
        Type(String rep) {
            stringRep = rep;
        }

        /**
         * Accessor for the operator type's string representation.
         *
         * @return the string representation of the operator type
         */
        public String stringRep() {
            return stringRep;
        }
    }


    /** Specifies the type of this Boolean operator. */
    private Type type;


    /**
     * The list of one or more terms in this expression.  If the expression type
     * is {@link BooleanOperator.Type#NOT_EXPR} then this will contain exactly
     * one term.
     */
    private ArrayList<Expression> terms;


    public BooleanOperator(Type type, Collection<Expression> terms) {

        if (type == null)
            throw new NullPointerException("type cannot be null");

        this.type = type;
        this.terms = new ArrayList<Expression>();

        if (terms != null) {
            for (Expression term : terms)
                addTerm(term);
        }
    }


    public BooleanOperator(Type type) {
        this(type, null);
    }


    /**
     * Returns the type of this Boolean expression.
     *
     * @return the type of this Boolean expression.
     */
    public Type getType() {
        return type;
    }


    public void addTerm(Expression term) {
        if (term == null)
            throw new NullPointerException("term cannot be null");

        terms.add(term);
    }


    public Expression getTerm(int i) {
        return terms.get(i);
    }


    public ColumnInfo getColumnInfo(Schema schema) throws SchemaNameException {
        // Boolean operators always return Boolean values, so just pass a
        // Boolean value in to the TypeConverter to get out the corresponding
        // SQL datatype.
        ColumnType colType =
            new ColumnType(TypeConverter.getSQLType(Boolean.FALSE));

        return new ColumnInfo(colType);
    }


    public Object evaluate(Environment env) throws ExpressionException {

        if (terms.size() == 0) {
            throw new ExpressionException("Boolean " + type +
                " expression contains no terms!");
        }

        Object objResult;
        boolean boolResult;

        if (type == Type.NOT_EXPR) {
            if (terms.size() != 1) {
                throw new ExpressionException(
                    "NOT-expressions may have only one term.");
            }

            // Evaluate the term...
            objResult = terms.get(0).evaluate(env);
            if (objResult == null)
                boolResult = false;  // TODO:  this is UNKNOWN, not FALSE.
            else
                boolResult = TypeConverter.getBooleanValue(objResult);

            // ...then negate it.
            boolResult = !boolResult;
        }
        else {
            // AND/OR expression.

            if (terms.size() < 1) {
                throw new ExpressionException(type.stringRep() +
                    "-expressions must have at least one term.");
            }

            if (type == Type.AND_EXPR) {
                boolResult = true;
            }
            else {
                assert type == Type.OR_EXPR : "Unexpected type value " + type + "!";
                boolResult = false;
            }

            for (Expression term : terms) {
                // Evaluate the i-th term, and combine it with the current answer.
                objResult = term.evaluate(env);

                boolean termValue;
                if (objResult == null)
                    termValue = false;  // TODO:  this is UNKNOWN, not FALSE.
                else
                    termValue = TypeConverter.getBooleanValue(objResult);

                if (type == Type.AND_EXPR && !termValue) {
                    // AND term is false, so we are done.  Answer is false.
                    boolResult = false;
                    break;
                }
                else if (type == Type.OR_EXPR && termValue) {
                    // OR term is true, so we are done.  Answer is true.
                    boolResult = true;
                    break;
                }
            }
        }

        return Boolean.valueOf(boolResult);
    }


    @Override
    public Expression traverse(ExpressionProcessor p) {
        p.enter(this);

        for (int i = 0; i < terms.size(); i++) {
            Expression e = terms.get(i).traverse(p);
            terms.set(i, e);
        }

        return p.leave(this);
    }


    /**
     * Returns a string representation of this Boolean logical expression and
     * its subexpressions, including parentheses where necessary to specify
     * precedence.
     */
    @Override
    public String toString() {
        // Convert all of the components into string representations.

        StringBuilder buf = new StringBuilder();

        if (type == Type.NOT_EXPR) {
            assert terms.size() == 1 : "NOT expressions must have exactly one term";

            buf.append(type.stringRep());
            buf.append(' ');

            Expression term = terms.get(0);
            String termStr = term.toString();
            if (term instanceof BooleanOperator) {
                BooleanOperator termOp = (BooleanOperator) term;
                if (termOp.type != Type.NOT_EXPR)
                    termStr = '(' + termStr + ')';
            }

            buf.append(termStr);
        }
        else if (type == Type.AND_EXPR) {
            assert terms.size() >= 1 : "AND expressions must have at least one term";

            boolean first = true;
                for (Expression term : terms) {
                if (first)
                    first = false;
                else
                    buf.append(' ').append(type.stringRep()).append(' ');

                String termStr = term.toString();

                if (term instanceof BooleanOperator) {
                    BooleanOperator termOp = (BooleanOperator) term;
                    if (termOp.type == Type.OR_EXPR)
                        termStr = '(' + termStr + ')';
                }

                buf.append(termStr);
            }
        }
        else {  // OR_EXPR
            assert type == Type.OR_EXPR;
            assert terms.size() >= 1 : "OR expressions must have at least one term";

            boolean first = true;
            for (Expression term : terms) {
                if (first)
                    first = false;
                else
                    buf.append(' ').append(type.stringRep()).append(' ');

                buf.append(term.toString());
            }
        }

        return buf.toString();
    }


    /**
     * This method returns true if this Boolean expression contains any terms
     * that reference the exact set of tables specified in the arguments.  See
     * the documentation for {@link #getTermsReferencingAllTables} for more
     * details on what is considered to be a "matching" term by this method.
     *
     * @design This method is <em>slightly</em> more efficient than the
     *         get/remove methods, since it stops after it has found the first
     *         term that satisfies the specified conditions.  However, if there
     *         will be a subsequent call to the get or remove method, it's
     *         probably most efficient to just call it directly.
     *
     * @param tableNames a collection of table-names to look for in the terms.
     *
     * @return A list of terms that reference all of the tables specified in the
     *         input list, and no others.
     */
    public boolean hasTermsReferencingAllTables(String... tableNames) {

        // Put table names into a set so we can easily do membership tests.
        HashSet<String> inputTables = new HashSet<String>();
        for (String tableName : tableNames)
            inputTables.add(tableName);

        // Iterate through each term in this Boolean operator.  For each term,
        // find out what symbols the term contains.  Then, see if the term only
        // references the tables in the input set.
        HashSet<ColumnName> symbols = new HashSet<ColumnName>();
        HashSet<String> exprTables = new HashSet<String>();
        for (Expression term : terms) {
            symbols.clear();
            exprTables.clear();

            term.getAllSymbols(symbols);
            for (ColumnName colName : symbols) {
                if (colName.isTableSpecified())
                    exprTables.add(colName.getTableName());
            }

            if (inputTables.equals(exprTables))
                return true;
        }

        return false;
    }


    /**
     * This method finds and returns a list of all terms in this Boolean
     * expression that reference the exact set of tables specified in the
     * arguments.  If a term references other tables outside of this set then it
     * will not be returned.  If a term doesn't reference some table in this set
     * then it will not be returned.
     *
     * @param tableNames a collection of table-names to look for in the terms.
     *
     * @return A list of terms that reference all of the tables specified in the
     *         input list, and no others.
     */
    public List<Expression> getTermsReferencingAllTables(String... tableNames) {
        // Call the helper, with "remove" flag set to false.
        return _getTermsReferencingAllTables(tableNames, false);
    }


    /**
     * This method finds, removes, and returns a list of all terms in this
     * Boolean expression that reference the exact set of tables specified in
     * the arguments.  If a term references other tables outside of this set
     * then it will not be returned.  If a term doesn't reference some table in
     * this set then it will not be returned.
     * <p>
     * The sole difference between this and {@link #getTermsReferencingAllTables}
     * is that this method also removes the found terms from this Boolean
     * expression object.
     *
     * @param tableNames a collection of table-names to look for in the terms.
     *
     * @return A list of terms that reference all of the tables specified in the
     *         input list, and no others.  These terms are also removed from the
     *         Boolean expression object.
     */
    public List<Expression> removeTermsReferencingAllTables(String... tableNames) {
        // Call the helper, with "remove" flag set to true.
        return _getTermsReferencingAllTables(tableNames, true);
    }


    /**
     * This is a private helper function used by both the
     * {@link #getTermsReferencingAllTables} and
     * {@link #removeTermsReferencingAllTables} methods.
     *
     * @param tableNames an array of table-names to look for in the terms.
     * @param remove if <tt>true</tt> then matching terms will be removed from
     *        this Boolean operator's list of terms.
     *
     * @return A list of terms that reference all of the tables specified in the
     *         input list, and no others.
     */
    private List<Expression> _getTermsReferencingAllTables(String[] tableNames,
        boolean remove) {

        ArrayList<Expression> found = new ArrayList<Expression>();

        // Put table names into a set so we can easily do membership tests.
        HashSet<String> inputTables = new HashSet<String>();
        for (String tableName : tableNames)
            inputTables.add(tableName);

        // Iterate through each term in this Boolean operator.  For each term,
        // find out what symbols the term contains.  Then, see if the term only
        // references the tables in the input set.
        HashSet<ColumnName> symbols = new HashSet<ColumnName>();
        HashSet<String> exprTables = new HashSet<String>();
        Iterator<Expression> termIter = terms.iterator();
        while (termIter.hasNext()) {
            Expression term = termIter.next();

            symbols.clear();
            exprTables.clear();

            term.getAllSymbols(symbols);
            for (ColumnName colName : symbols) {
                if (colName.isTableSpecified())
                    exprTables.add(colName.getTableName());
            }

            if (inputTables.equals(exprTables)) {
                found.add(term);
                if (remove)
                    termIter.remove();
            }
        }

        return found;
    }


    /**
     * Returns the number of terms in the boolean expression.
     *
     * @return the number of terms in the boolean expression.
     */
    public int getNumTerms() {
        return terms.size();
    }


    /**
     * Removes the i<sup>th</sup> term, starting from 0.
     *
     * @param i the index of the term to remove.
     *
     * @throws IllegalArgumentException if the specified index is invalid.
     */
    public void removeTerm(int i) {
        if (i < 0 || i >= getNumTerms()) {
            throw new IllegalArgumentException("Term-index " + i +
                " is out of range [0, " + getNumTerms() + ")");
        }

        terms.remove(i);
    }


    /**
     * Performs a value-eqality test for whether the specified object is an
     * expression with the same structure and contents.
     *
     * @param obj the object to which we are comparing
     */
    @Override
    public boolean equals(Object obj) {
        if (obj instanceof BooleanOperator) {
            BooleanOperator other = (BooleanOperator) obj;
    
            return (type == other.type && terms.equals(other.terms));
        }
        return false;
    }
  
  
    /**
     * Computes the hash-code of an Expression.
     */
    @Override
    public int hashCode() {
        int hash = 7;
        hash = 31 * hash + type.hashCode();
        hash = 31 * hash + terms.hashCode();
        return hash;
    }


    /**
     * Creates a copy of expression.
     */
    @Override
    @SuppressWarnings("unchecked")
    protected Object clone() throws CloneNotSupportedException {
        BooleanOperator expr = (BooleanOperator) super.clone();

        // Type is immutable, copy it.
        expr.type = this.type;

        expr.terms = (ArrayList<Expression>) terms.clone();

        return expr;
    }
}
