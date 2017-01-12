package edu.caltech.nanodb.expressions;


import java.util.HashMap;

import edu.caltech.nanodb.relations.ColumnType;
import edu.caltech.nanodb.relations.SQLDataType;


/**
 * This class provides a whole bunch of helper functions for performing type
 * conversions on values produced by various expressions.  All values are passed
 * around as <code>Object</code> references, and this means they may need to be
 * converted into specific value types at some point.  This class contains all
 * of the conversion logic so it's in one place.
 *
 * @todo Update these methods to support <tt>NULL</tt> (i.e. Java <tt>null</tt>)
 *       values.  Right now we just throw a type-cast exception.
 */
public class TypeConverter {

    /**
     * A mapping from the various Java types used for expression results, to their
     * corresponding SQL data types.  The mapping is populated by a static
     * initializer block.
     */
    private static HashMap<Class, SQLDataType> sqlTypeMapping;

    static {
        sqlTypeMapping = new HashMap<Class, SQLDataType>();

        sqlTypeMapping.put(Null.class, SQLDataType.NULL);

        sqlTypeMapping.put(Boolean.class, SQLDataType.TINYINT);

        sqlTypeMapping.put(Byte.class, SQLDataType.TINYINT);
        sqlTypeMapping.put(Short.class, SQLDataType.SMALLINT);
        sqlTypeMapping.put(Integer.class, SQLDataType.INTEGER);
        sqlTypeMapping.put(Long.class, SQLDataType.BIGINT);

        sqlTypeMapping.put(Float.class, SQLDataType.FLOAT);
        sqlTypeMapping.put(Double.class, SQLDataType.DOUBLE);

        sqlTypeMapping.put(String.class, SQLDataType.VARCHAR);

        // TODO:  others, in time...
    }


    /**
     * A simple wrapper-class for holding two values.  This is used to return
     * results from the coercion functions, since they take two inputs and must
     * coerce them into being the same types.
     */
    public static class Pair {
        /** The first value of the pair. */
        public Object value1;

        /** The second value of the pair. */
        public Object value2;

        /**
         * Construct a new pair using the specified values.
         *
         * @param obj1 the first value to store
         * @param obj2 the second value to store
         */
        public Pair(Object obj1, Object obj2) {
            value1 = obj1;
            value2 = obj2;
        }
    }


    /**
     * This method attempts to convert the input value into a
     * {@link java.lang.Boolean} value.  If the input is a nonzero number then
     * the result is {@link java.lang.Boolean#TRUE}; if it is a zero number then
     * the result is {@link java.lang.Boolean#FALSE}.  Otherwise a
     * {@link TypeCastException} is thrown.
     *
     * @param obj the input value to cast
     *
     * @return the input value cast to a <tt>Boolean</tt>
     *
     * @throws TypeCastException if the input value cannot be cast to
     *         a Boolean value.
     */
    public static Boolean getBooleanValue(Object obj) {
        if (obj == null)
            return null;

        Boolean result;

        if (obj instanceof Boolean) {
            result = (Boolean) obj;
        }
        else if (obj instanceof Number) {
            // If it's a nonzero number, return TRUE.  Otherwise, FALSE.
            Number num = (Number) obj;
            result = Boolean.valueOf(num.intValue() != 0);
        }
        else {
            throw new TypeCastException("Cannot convert type " +
                obj.getClass() + " to byte.");
        }

        return result;
    }


    /**
     * This method attempts to convert the input value into a
     * {@link java.lang.Byte} value.  If the input is a number then the result
     * is generated from the {@link java.lang.Number#byteValue} method, possibly
     * causing truncation or overflow to occur.  If the input is a
     * {@link java.lang.String} that can be parsed into a byte then the result
     * is the parsed value.  If none of these cases hold then a
     * {@link TypeCastException} is thrown.
     *
     * @param obj the input value to cast
     *
     * @return the input value cast to a <tt>Byte</tt>
     *
     * @throws TypeCastException if the input value cannot be cast to a byte.
     */
    public static Byte getByteValue(Object obj) {
        if (obj == null)
            return null;

        Byte result;

        if (obj instanceof Byte) {
            result = (Byte) obj;
        }
        else if (obj instanceof Number) {
            // TODO:  Would be nice to detect overflow or truncation issues and
            //        log warnings about them!

            Number num = (Number) obj;
            result = Byte.valueOf(num.byteValue());
        }
        else if (obj instanceof String) {
            try {
                result = Byte.valueOf((String) obj);
            }
            catch (NumberFormatException nfe) {
                throw new TypeCastException("Cannot convert string to byte.", nfe);
            }
        }
        else {
            throw new TypeCastException("Cannot convert type \"" +
                obj.getClass() + "\" to byte.");
        }

        return result;
    }


    /**
     * This method attempts to convert the input value into a
     * {@link java.lang.Short} value.  If the input is a number then the result
     * is generated from the {@link java.lang.Number#shortValue} method,
     * possibly causing truncation or overflow to occur.  If the input is a
     * {@link java.lang.String} that can be parsed into a short then the result
     * is the parsed value.  If none of these cases hold then a
     * {@link TypeCastException} is thrown.
     *
     * @param obj the input value to cast
     *
     * @return the input value cast to a <tt>Short</tt>
     *
     * @throws TypeCastException if the input value cannot be cast to a short.
     */
    public static Short getShortValue(Object obj) {
        if (obj == null)
            return null;

        Short result;

        if (obj instanceof Short) {
            result = (Short) obj;
        }
        else if (obj instanceof Number) {
            // TODO:  Would be nice to detect overflow or truncation issues and log
            //        warnings about them!

            Number num = (Number) obj;
            result = Short.valueOf(num.shortValue());
        }
        else if (obj instanceof String) {
            try {
                result = Short.valueOf((String) obj);
            }
            catch (NumberFormatException nfe) {
                throw new TypeCastException("Cannot convert string to short.", nfe);
            }
        }
        else {
            throw new TypeCastException("Cannot convert type \"" +
                obj.getClass() + "\" to short.");
        }

        return result;
    }


    /**
     * This method attempts to convert the input value into an
     * {@link java.lang.Integer} value.  If the input is a number then the
     * result is generated from the {@link java.lang.Number#intValue} method,
     * possibly causing truncation or overflow to occur.  If the input is a
     * {@link java.lang.String} that can be parsed into an integer then the
     * result is the parsed value.  If none of these cases hold then a
     * {@link TypeCastException} is thrown.
     *
     * @param obj the input value to cast
     *
     * @return the input value cast to an <tt>Integer</tt>
     *
     * @throws TypeCastException if the input value cannot be cast to an integer.
     */
    public static Integer getIntegerValue(Object obj) {
        if (obj == null)
            return null;

        Integer result;

        if (obj instanceof Integer) {
            result = (Integer) obj;
        }
        else if (obj instanceof Number) {
            // TODO:  Would be nice to detect overflow or truncation issues and log
            //        warnings about them!

            Number num = (Number) obj;
            result = Integer.valueOf(num.intValue());
        }
        else if (obj instanceof String) {
            try {
                result = Integer.valueOf((String) obj);
            }
            catch (NumberFormatException nfe) {
                throw new TypeCastException("Cannot convert string to integer.", nfe);
            }
        }
        else {
            throw new TypeCastException("Cannot convert type \"" +
                obj.getClass() + "\" to integer.");
        }

        return result;
    }


    /**
     * This method attempts to convert the input value into a
     * {@link java.lang.Long} value.  If the input is a number then the result
     * is generated from the {@link java.lang.Number#longValue} method, possibly
     * causing truncation or overflow to occur.  If the input is a
     * {@link java.lang.String} that can be parsed into a long then the result
     * is the parsed value.  If none of these cases hold then a
     * {@link TypeCastException} is thrown.
     *
     * @param obj the input value to cast
     *
     * @return the input value cast to a <tt>Long</tt>
     *
     * @throws TypeCastException if the input value cannot be cast to a long.
     */
    public static Long getLongValue(Object obj) {
        if (obj == null)
            return null;

        Long result;

        if (obj instanceof Long) {
            result = (Long) obj;
        }
        else if (obj instanceof Number) {
            // TODO:  Would be nice to detect overflow or truncation issues and log
            //        warnings about them!

            Number num = (Number) obj;
            result = Long.valueOf(num.longValue());
        }
        else if (obj instanceof String) {
            try {
                result = Long.valueOf((String) obj);
            }
            catch (NumberFormatException nfe) {
                throw new TypeCastException("Cannot convert string to long.", nfe);
            }
        }
        else {
            throw new TypeCastException("Cannot convert type \"" +
                obj.getClass() + "\" to long.");
        }

        return result;
    }


    /**
     * This method attempts to convert the input value into a
     * {@link java.lang.Float} value.  If the input is a number then the result
     * is generated from the {@link java.lang.Number#floatValue} method,
     * possibly causing truncation or overflow to occur.  If the input is a
     * {@link java.lang.String} that can be parsed into a float then the result
     * is the parsed value.  If none of these cases hold then a
     * {@link TypeCastException} is thrown.
     *
     * @param obj the input value to cast
     *
     * @return the input value cast to a <tt>Float</tt>
     *
     * @throws TypeCastException if the input value cannot be cast to a float.
     */
    public static Float getFloatValue(Object obj) {
        if (obj == null)
            return null;

        Float result;

        if (obj instanceof Float) {
            result = (Float) obj;
        }
        else if (obj instanceof Number) {
            // TODO:  Would be nice to detect overflow or truncation issues and log
            //        warnings about them!

            Number num = (Number) obj;
            result = Float.valueOf(num.floatValue());
        }
        else if (obj instanceof String) {
            try {
                result = new Float((String) obj);
            }
            catch (NumberFormatException nfe) {
                throw new TypeCastException("Cannot convert string to float.", nfe);
            }
        }
        else {
            throw new TypeCastException("Cannot convert type \"" +
                obj.getClass() + "\" to float.");
        }

        return result;
    }


    /**
     * This method attempts to convert the input value into a
     * {@link java.lang.Double} value.  If the input is a number then the result
     * is generated from the {@link java.lang.Number#doubleValue} method,
     * possibly causing truncation or overflow to occur.  If the input is a
     * {@link java.lang.String} that can be parsed into a double then the result
     * is the parsed value.  If none of these cases hold then a
     * {@link TypeCastException} is thrown.
     *
     * @param obj the input value to cast
     *
     * @return the input value cast to a <tt>Double</tt>
     *
     * @throws TypeCastException if the input value cannot be cast to a double.
     */
    public static Double getDoubleValue(Object obj) {
        if (obj == null)
            return null;

        Double result;

        if (obj instanceof Double) {
            result = (Double) obj;
        }
        else if (obj instanceof Number) {
            // This is the only conversion that doesn't have the chance of
            // truncating or overflowing, AFAIK.

            Number num = (Number) obj;
            result = Double.valueOf(num.doubleValue());
        }
        else if (obj instanceof String) {
            try {
                result = new Double((String) obj);
            }
            catch (NumberFormatException nfe) {
                throw new TypeCastException("Cannot convert string to double.", nfe);
            }
        }
        else {
            throw new TypeCastException("Cannot convert type \"" +
                obj.getClass() + "\" to double.");
        }

        return result;
    }


    /**
     * This method converts the input value into a {@link java.lang.String}
     * value by calling {@link java.lang.Object#toString} on the input.
     *
     * @param obj the input value to cast
     *
     * @return the input value cast to a <tt>String</tt>
     */
    public static String getStringValue(Object obj) {
        // If obj is a String, well, String.toString() just returns itself!  :-)
        return (obj != null ? obj.toString() : null);
    }


    /**
     * This function takes two arguments and coerces them to be the same numeric
     * type, for use with arithmetic operations.
     * <p>
     * If either or both of the arguments are <tt>null</tt> then no coercion is
     * performed, and the results are returned in a pair-object.  The reason for
     * this is that the comparison will simply evaluate to <tt>UNKNOWN</tt>, so
     * no coercion is required.
     *
     * @param obj1 the first input value to cast
     * @param obj2 the second input value to cast
     *
     * @return an object holding the two input values, both converted to a type
     *         suitable for arithmetic
     *
     * @throws TypeCastException if both inputs are non-<tt>null</tt> and they
     *         cannot both be cast to types suitable for arithmetic.
     */
    public static Pair coerceArithmetic(Object obj1, Object obj2)
        throws TypeCastException {

        if (obj1 != null && obj2 != null) {
            if (obj1 instanceof Number || obj2 instanceof Number) {
                if (obj1 instanceof Double || obj2 instanceof Double) {
                    // At least one is a Double, so convert both to Doubles.
                    obj1 = getDoubleValue(obj1);
                    obj2 = getDoubleValue(obj2);
                }
                else if (obj1 instanceof Float || obj2 instanceof Float) {
                    // At least one is a Float, so convert both to Floats.
                    obj1 = getFloatValue(obj1);
                    obj2 = getFloatValue(obj2);
                }
                else if (obj1 instanceof Long || obj2 instanceof Long) {
                    // At least one is a Long, so convert both to Longs.
                    obj1 = getLongValue(obj1);
                    obj2 = getLongValue(obj2);
                }
                else {
                    // Any other integer-type we will just coerce into being Integers.
                    obj1 = getIntegerValue(obj1);
                    obj2 = getIntegerValue(obj2);
                }
            }
            else {
                throw new TypeCastException(String.format(
                    "Cannot coerce types \"%s\" and \"%s\" for arithmetic.",
                    obj1.getClass(), obj2.getClass()));
            }

            assert obj1.getClass().equals(obj2.getClass());
        }

        return new Pair(obj1, obj2);
    }


    /**
     * This function takes two arguments and coerces them to be the same numeric
     * type, for use with comparison operations.  It is up to the caller to
     * ensure that the types are actually comparable (although all recognized
     * types in this function are comparable).
     * <p>
     * If either or both of the arguments are <tt>null</tt> then no coercion is
     * performed, and the results are returned in a pair-object.  The reason for
     * this is that the comparison will simply evaluate to <tt>UNKNOWN</tt>, so
     * no coercion is required.
     *
     * @design When new types are added in the future (e.g. date and/or time
     *         values), this function will need to be updated with the new types.
     *
     * @param obj1 the first input value to cast
     * @param obj2 the second input value to cast
     *
     * @return an object holding the two input values, both converted to a type
     *         suitable for comparison
     *
     * @throws TypeCastException if both inputs are non-<tt>null</tt> and they
     *         cannot both be cast to types suitable for comparison.
     */
    public static Pair coerceComparison(Object obj1, Object obj2)
        throws TypeCastException {

        if (obj1 != null && obj2 != null) {
            if (obj1.getClass().equals(obj2.getClass())) {
                // The two objects are already of the same type, so no need
                // to coerce anything.
            }
            else if (obj1 instanceof Number || obj2 instanceof Number) {
                // Reuse the same logic in the arithmetic coercion code.
                return coerceArithmetic(obj1, obj2);
            }
            else if (obj1 instanceof Boolean || obj2 instanceof Boolean) {
                obj1 = getBooleanValue(obj1);
                obj2 = getBooleanValue(obj2);
            }
            else if (obj1 instanceof String || obj2 instanceof String) {
                obj1 = getStringValue(obj1);
                obj2 = getStringValue(obj2);
            }
            else {
                // Inputs are different types, and we don't know how to make
                // them the same.
                throw new TypeCastException(String.format(
                    "Cannot coerce types \"%s\" and \"%s\" for comparison.",
                    obj1.getClass(), obj2.getClass()));
            }

            assert obj1.getClass().equals(obj2.getClass());
        }

        return new Pair(obj1, obj2);
    }


    /**
     * This method attempts to assign a SQL data type to a value produced by the
     * expression classes.  If a Java type is not recognized as a particular SQL
     * data type then <tt>null</tt> is returned.
     *
     * @param obj The object to determine a SQL data type for.
     *
     * @return The corresponding SQL data type, or <tt>null</tt> if the input
     *         value's type doesn't have an obvious corresponding SQL data type.
     */
    public static SQLDataType getSQLType(Object obj) {
        if (obj == null)
            return sqlTypeMapping.get(Null.class);

        return sqlTypeMapping.get(obj.getClass());
    }


    /**
     * This method attempts to create a
     * {@link edu.caltech.nanodb.relations.ColumnType} object for a value
     * produced by the expression classes.  If the Java type is not recognized
     * as a particular SQL data type then <tt>null</tt> is returned.  If the
     * corresponding SQL data type has a length or precision, this method sets
     * that information appropriately on the <tt>ColumnType</tt> object.
     * <p>
     * This method uses the {@link #getSQLType} method to determine the SQL data
     * type, so the two methods are consistent with each other.
     *
     * @param obj The object to create a <tt>ColumnType</tt> object for.
     *
     * @return The corresponding <tt>ColumnType</tt> object, or <tt>null</tt> if
     *         the input value's type doesn't have an obvious corresponding SQL
     *         data type.
     */
    public static ColumnType getColumnType(Object obj) {
        ColumnType colType = null;

        SQLDataType sqlType = getSQLType(obj);
        if (sqlType != null) {
            colType = new ColumnType(sqlType);

            if (sqlType == SQLDataType.VARCHAR)
                colType.setLength(obj.toString().length());
        }

        return colType;
    }
}
