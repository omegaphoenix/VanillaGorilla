package edu.caltech.nanodb.functions;


/**
 * This is the root class of all kinds of functions in NanoDB.  The class
 * hierarchy for functions is somewhat complicated; these are the important
 * details:
 * <ul>
 *   <li>{@link ScalarFunction} is the parent class for any kind of function
 *       that returns a scalar value
 *       <ul>
 *           <li>{@link SimpleFunction} is the parent class for functions that
 *               take zero or more arguments, and compute and return a result
 *           </li>
 *           <li>{@link AggregateFunction} is the parent class for functions
 *               that consume a (potentially large) collection of input values
 *               and compute a single aggregated value from the collection
 *           </li>
 *       </ul>
 *   </li>
 *   <li>{@link TableFunction} is the parent class for table-returning
 *       functions</li>
 * </ul>
 *
 * Functions must support cloning because the implementation classes often
 * carry their own internal state values, and clearly the same function being
 * invoked in two different parts of the same query, or being invoked
 * concurrently by two different queries, shouldn't have a single shared set
 * of state values.  So, the simple thing to do is to just clone functions
 * when they are retrieved from the {@link FunctionDirectory}.
 */
public abstract class Function implements Cloneable {

    /**
     * Creates a copy of expression.
     */
    @Override
    public Object clone() throws CloneNotSupportedException {
        return super.clone();
    }
}
