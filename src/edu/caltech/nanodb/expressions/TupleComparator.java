package edu.caltech.nanodb.expressions;


import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import edu.caltech.nanodb.relations.Schema;
import edu.caltech.nanodb.relations.Tuple;


/**
 * This class allows us to sort and compare tuples based on an order-by
 * specification.  The specification is simply a list of
 * {@link OrderByExpression} objects, and the order of the expressions
 * themselves matters.  Tuples will be ordered by the first expression; if the
 * tuples' values are the same then the tuples will be ordered by the second
 * expression; etc.
 */
public class TupleComparator implements Comparator<Tuple> {

    /**
     * The schema of the tuples that will be compared by this comparator object.
     */
    private Schema schema;


    /** The specification of how to order the tuples being compared. */
    private ArrayList<OrderByExpression> orderSpec;


    /**
     * The environment to use for evaluating order-by expressions against the
     * first tuple.
     */
    private Environment envTupleA = new Environment();


    /**
     * The environment to use for evaluating order-by expressions against the
     * second tuple.
     */
    private Environment envTupleB = new Environment();


    /** The epsilon used to compare floating-point values. */
    private static double EPSILON = 0.00000;


    /**
     * Construct a new tuple-comparator with the given ordering specification.
     *
     * @param schema the schema of the tuples that will be compared by this
     *        comparator object
     *
     * @param orderSpec a series of order-by expressions used to order the
     *        tuples being compared
     */
    public TupleComparator(Schema schema, List<OrderByExpression> orderSpec) {
        if (schema == null)
            throw new IllegalArgumentException("schema cannot be null");

        if (orderSpec == null)
            throw new IllegalArgumentException("orderSpec cannot be null");

        this.schema = schema;
        this.orderSpec = new ArrayList<OrderByExpression>(orderSpec);
    }


    /**
     * Performs the comparison of two tuples based on the configuration of this
     * tuple-comparator object.
     *
     * @design (Donnie) We have to suppress "unchecked operation" warnings on
     *         this code, since {@link Comparable} is a generic (and thus allows
     *         us to specify the type of object being compared), but we want to
     *         use it without specifying any types.
     *
     * @param a the first tuple to compare.
     * @param b the second tuple to compare.
     * @return a negative, zero, or positive value, corresponding to whether
     *         tuple <tt>a</tt> is less than, equal to, or greater than tuple
     *         <tt>b</tt>.
     */
    @Override
    @SuppressWarnings("unchecked")
    public int compare(Tuple a, Tuple b) {

        // Set up the environments for evaluating the order-by specifications.

        envTupleA.clear();
        envTupleA.addTuple(schema, a);

        envTupleB.clear();
        envTupleB.addTuple(schema, b);

        int compareResult = 0;

        // For each order-by spec, evaluate the expression against both tuples,
        // and compare the results.
        for (OrderByExpression entry : orderSpec) {
            Expression expr = entry.getExpression();

            Comparable valueA = (Comparable) expr.evaluate(envTupleA);
            Comparable valueB = (Comparable) expr.evaluate(envTupleB);

            // Although it should be "unknown" when we compare two NULL values
            // for equality, we say they are equal so that they will all appear
            // together in the sorting results.
            if (valueA == null) {
                if (valueB != null)
                    compareResult = -1;
                else
                    compareResult = 0;
            }
            else if (valueB == null) {
                compareResult = 1;
            }
            else {
                compareResult = valueA.compareTo(valueB);
            }

            if (compareResult != 0) {
                if (!entry.isAscending())
                    compareResult = -compareResult;

                break;
            }
        }

        return compareResult;
    }


    /**
    * This helper function returns true if two tuples have the same number of
    * columns and the values compare as equal when coerced with the
    * {@link TypeConverter#coerceComparison} method.  Note that the schemas of
    * the tuples are not considered.
    *
    * @param t1 the first tuple to compare
    * @param t2 the second tuple to compare
    * @param epsilon an optional argument if the comparisions do not have to be
    *                extremely exact (used for tests)
    * @return true if the two tuples have the same number of columns, and the
    *         values from <tt>t1</tt> and <tt>t2</tt> compare equal.
    */
    public static boolean areTuplesEqual(Tuple t1, Tuple t2, double... epsilon) {
        if (t1 == null)
            throw new IllegalArgumentException("t1 cannot be null");

        if (t2 == null)
            throw new IllegalArgumentException("t2 cannot be null");

        if (t1.getColumnCount() != t2.getColumnCount())
            return false;

        // Set the epsilon value
        if (epsilon.length == 1)
            EPSILON = epsilon[0];

        int size = t1.getColumnCount();
        for (int i = 0; i < size; i++) {
            Object obj1 = t1.getColumnValue(i);
            Object obj2 = t2.getColumnValue(i);

            if (obj1 == null) {
                if (obj2 != null) {
                    // obj1 is null, but obj2 isn't.
                    return false;
                }

                // If we got here, both obj1 and obj2 are null.
            }
            else if (obj2 == null) {
                // obj1 isn't null, but obj2 is.
                return false;
            }
            else {
                // Both objects are non-null.
                TypeConverter.Pair p =
                TypeConverter.coerceComparison(obj1, obj2);

                // Handle cases with errors in floating-point precision
                if (p.value1 instanceof Double) {
                    double value1 = ((Double)p.value1).doubleValue();
                    double value2 = ((Double)p.value2).doubleValue();
                    if (Math.abs(value1 - value2) > EPSILON)
                        return false;
                }
                else if (p.value1 instanceof Float) {
                    float value1 = ((Float)p.value1).floatValue();
                    float value2 = ((Float)p.value2).floatValue();
                    if (Math.abs(value1 - value2) > EPSILON)
                        return false;
                }

                else if (!p.value1.equals(p.value2))
                    return false;
            }
        }

        return true;
    }


    /**
     * <p>
     * Compares all columns of the two input tuples, in the order they appear
     * within the tuples, and a value is returned to indicate the ordering.
     * </p>
     * <ul>
     *   <li>Result &lt; 0 if <tt>t1</tt> &lt; <tt>t2</tt></li>
     *   <li>Result == 0 if <tt>t1</tt> == <tt>t2</tt></li>
     *   <li>Result &gt; 0 if <tt>t1</tt> &gt; <tt>t2</tt></li>
     * </ul>
     * <p>
     * This method performs type-coercion between the values being compared, so
     * that it may be used for evaluating general predicates.
     * </p>
     * <p>
     * <b>This method requires the tuples being compared to have the same number
     * of columns.</b>  For a method that allows tuples with different numbers
     * of columns to be compared, see the {@link #comparePartialTuples} method.
     * </p>
     *
     * @param t1 the first tuple to compare.  Must not be {@code null}.
     * @param t2 the second tuple to compare.  Must not be {@code null}.
     *
     * @return a negative, positive, or zero value indicating the ordering of
     *         the two inputs
     *
     * @throws IllegalArgumentException if the two input tuples are different
     *         sizes.
     */
    public static int compareTuples(Tuple t1, Tuple t2) {
        return _compareTuples(t1, t2, false);
    }


    /**
     * <p>
     * Compares all columns of the two input tuples, in the order they appear
     * within the tuples, and a value is returned to indicate the ordering.
     * </p>
     * <ul>
     *   <li>Result &lt; 0 if <tt>t1</tt> &lt; <tt>t2</tt></li>
     *   <li>Result == 0 if <tt>t1</tt> == <tt>t2</tt></li>
     *   <li>Result &gt; 0 if <tt>t1</tt> &gt; <tt>t2</tt></li>
     * </ul>
     * <p>
     * This method performs type-coercion between the values being compared, so
     * that it may be used for evaluating general predicates.
     * </p>
     * <p>
     * <b>This method differs from the {@link #compareTuples} method in that
     * it allows {@code t1} and {@code t2} to have different numbers of
     * columns.</b>  Tuples are compared in a way similar to strings, where if
     * two different-length strings have the same values in the overlapping
     * portion, the shorter string is defined to be "less than" the longer
     * string.  This allows this comparison method to be used to do partial-key
     * lookups against ordered indexes.
     * </p>
     *
     * @param t1 the first tuple to compare.  Must not be {@code null}.
     * @param t2 the second tuple to compare.  Must not be {@code null}.
     *
     * @return a negative, positive, or zero value indicating the ordering of
     *         the two inputs
     */
    public static int comparePartialTuples(Tuple t1, Tuple t2) {
        return _compareTuples(t1, t2, true);
    }


    /**
     * This is the private helper function that implements both the
     * {@link #compareTuples} method and the {@link #comparePartialTuples}
     * method.  Its behavior with respect to tuples with different numbers of
     * columns is controlled with a Boolean argument.
     *
     * @param t1 the first tuple to compare.  Must not be {@code null}.
     *
     * @param t2 the second tuple to compare.  Must not be {@code null}.
     *
     * @param allowSizeMismatch true if the two tuples are allowed to be
     *        different sizes, or false if they must be the same size.
     *
     * @return a negative, positive, or zero value indicating the ordering of
     *         the two inputs
     */
    @SuppressWarnings("unchecked")
    private static int _compareTuples(Tuple t1, Tuple t2,
                                      boolean allowSizeMismatch) {

        int t1Size = t1.getColumnCount();
        int t2Size = t2.getColumnCount();

        if (!allowSizeMismatch) {
            if (t1Size != t2Size)
                throw new IllegalArgumentException("tuples must be the same size");
        }
        else {
            // If one of the tuples is an empty tuple, and the other one is
            // not, we define this as the empty tuple being less than the
            // non-empty tuple.
            if (t1Size == 0 || t2Size == 0)
                return t1Size - t2Size;

            // Now we know that both tuples have at least one column.
            // Only compare the columns that are present in both tuples.
            t1Size = Math.min(t1Size, t2Size);
            t2Size = t1Size;
        }

        int compareResult = 0;

        // TODO:  Rework this logic once we are confident with the comparison change.
        int i = 0;
        while (compareResult == 0) {
            // Examine the sizes first.
            if (i >= t1Size) {
                if (i >= t2Size) {
                    // Everything in t1[0..t1Size) == t2[0..t1Size), and
                    // t1Size == t2Size.  So, t1 == t2.

                    assert compareResult == 0;
                    assert t1Size == t2Size;

                    break;
                }
                else {
                    // Everything in t1[0..t1Size) == t2[0..t1Size), but
                    // t1 is shorter than t2.  So, t1 < t2.
                    compareResult = -1;
                    break;
                }
            }
            else if ( /* i < t1Size && */ i >= t2Size) {
                // Everything in t1[0..t2Size) == t2[0..t2Size), but
                // t2 is shorter than t1.  So, t1 > t2.
                compareResult = 1;
                break;
            }

            // Now examine the values.

            Object objA = t1.getColumnValue(i);
            Object objB = t2.getColumnValue(i);

            TypeConverter.Pair p = TypeConverter.coerceComparison(objA, objB);

            Comparable valueA = (Comparable) objA;
            Comparable valueB = (Comparable) objB;

            // Although it should be "unknown" when we compare two NULL values
            // for equality, we say they are equal so that they will all appear
            // together in the sorting results.
            if (valueA == null) {
                if (valueB != null)
                    compareResult = -1;
                else
                    compareResult = 0;
            }
            else if ( /* valueA != null && */ valueB == null) {
                compareResult = 1;
            }
            else {
                compareResult = valueA.compareTo(valueB);
            }

            i++;
        }

        return compareResult;
    }
}
