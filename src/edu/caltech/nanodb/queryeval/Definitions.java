package edu.caltech.nanodb.queryeval;


import edu.caltech.nanodb.queryast.SelectClause;
import edu.caltech.nanodb.expressions.Expression;

import java.util.HashMap;


/**
 * This class holds expression aliases and view definitions during query
 * planning.
 */
public class Definitions {
    /** A collection of expression aliases. */
    private HashMap<String, Expression> expressionAliases;

    /**
     * A collection of view definitions from the query, specified as
     * <tt>WITH</tt> clauses.
     */
    private HashMap<String, SelectClause> viewDefinitions;


}
