package edu.caltech.test.nanodb.expressions;


import org.testng.annotations.*;

import edu.caltech.nanodb.expressions.TypeCastException;
import edu.caltech.nanodb.expressions.TypeConverter;
import edu.caltech.nanodb.relations.SQLDataType;


/**
 * This class exercises the type-converter class.
 */
@Test
public class TestTypeConverter {

    public void testGetBooleanValue() {
        assert Boolean.TRUE.equals(TypeConverter.getBooleanValue(new Integer(3)));
        assert Boolean.TRUE.equals(TypeConverter.getBooleanValue(Boolean.TRUE));

        assert Boolean.FALSE.equals(TypeConverter.getBooleanValue(new Integer(0)));
        assert Boolean.FALSE.equals(TypeConverter.getBooleanValue(Boolean.FALSE));

        assert null == TypeConverter.getBooleanValue(null);
    }

    @Test(expectedExceptions={TypeCastException.class})
    public void testGetBooleanValueError() {
        TypeConverter.getBooleanValue(new Object());
    }


    public void testGetByteValue() {
        assert TypeConverter.getByteValue(null) == null;

        assert Byte.valueOf((byte) 11).equals(TypeConverter.getByteValue(Byte.valueOf((byte) 11)));

        assert Byte.valueOf((byte) 3).equals(TypeConverter.getByteValue(new Integer(3)));
        assert Byte.valueOf((byte) 15).equals(TypeConverter.getByteValue("15"));
        assert Byte.valueOf((byte) 21).equals(TypeConverter.getByteValue(new Float(21.234)));
    }

    @Test(expectedExceptions={TypeCastException.class})
    public void testGetByteFromBooleanError() {
        TypeConverter.getByteValue(Boolean.TRUE);
    }

    @Test(expectedExceptions={TypeCastException.class})
    public void testGetByteFromObjectError() {
        TypeConverter.getByteValue(new Object());
    }

    @Test(expectedExceptions={TypeCastException.class})
    public void testGetByteFromStringError() {
        TypeConverter.getByteValue("123a");
    }


    public void testGetShortValue() {
        assert TypeConverter.getShortValue(null) == null;

        assert Short.valueOf((short) 11).equals(TypeConverter.getShortValue(Short.valueOf((short) 11)));

        assert Short.valueOf((short) 3).equals(TypeConverter.getShortValue(new Integer(3)));
        assert Short.valueOf((short) 15).equals(TypeConverter.getShortValue("15"));
        assert Short.valueOf((short) 21).equals(TypeConverter.getShortValue(new Float(21.234)));
    }

    @Test(expectedExceptions={TypeCastException.class})
    public void testGetShortFromBooleanError() {
        TypeConverter.getShortValue(Boolean.TRUE);
    }

    @Test(expectedExceptions={TypeCastException.class})
    public void testGetShortFromObjectError() {
        TypeConverter.getShortValue(new Object());
    }

    @Test(expectedExceptions={TypeCastException.class})
    public void testGetShortFromStringError() {
        TypeConverter.getShortValue("123a");
    }


    public void testGetIntegerValue() {
        assert TypeConverter.getIntegerValue(null) == null;

        assert Integer.valueOf(11).equals(TypeConverter.getIntegerValue(Integer.valueOf(11)));

        assert Integer.valueOf(3).equals(TypeConverter.getIntegerValue(new Long(3)));
        assert Integer.valueOf(15).equals(TypeConverter.getIntegerValue("15"));
        assert Integer.valueOf(21).equals(TypeConverter.getIntegerValue(new Float(21.234)));
    }

    @Test(expectedExceptions={TypeCastException.class})
    public void testGetIntegerFromBooleanError() {
        TypeConverter.getIntegerValue(Boolean.TRUE);
    }

    @Test(expectedExceptions={TypeCastException.class})
    public void testGetIntegerFromObjectError() {
        TypeConverter.getIntegerValue(new Object());
    }

    @Test(expectedExceptions={TypeCastException.class})
    public void testGetIntegerFromStringError() {
        TypeConverter.getIntegerValue("123a");
    }


    public void testGetLongValue() {
        assert TypeConverter.getLongValue(null) == null;

        assert Long.valueOf(11).equals(TypeConverter.getLongValue(Long.valueOf(11)));

        assert Long.valueOf(3).equals(TypeConverter.getLongValue(new Integer(3)));
        assert Long.valueOf(15).equals(TypeConverter.getLongValue("15"));
        assert Long.valueOf(21).equals(TypeConverter.getLongValue(new Float(21.234)));
    }

    @Test(expectedExceptions={TypeCastException.class})
    public void testGetLongFromBooleanError() {
        TypeConverter.getLongValue(Boolean.TRUE);
    }

    @Test(expectedExceptions={TypeCastException.class})
    public void testGetLongFromObjectError() {
        TypeConverter.getLongValue(new Object());
    }

    @Test(expectedExceptions={TypeCastException.class})
    public void testGetLongFromStringError() {
        TypeConverter.getLongValue("123a");
    }


    public void testGetFloatValue() {
        assert TypeConverter.getFloatValue(null) == null;

        assert new Float(11.125).equals(TypeConverter.getFloatValue(new Float(11.125)));

        assert Float.valueOf(3).equals(TypeConverter.getFloatValue(new Integer(3)));
        assert new Float(15.2579).equals(TypeConverter.getFloatValue("15.2579"));
        assert new Float(21.234).equals(TypeConverter.getFloatValue(new Float(21.234)));
    }

    @Test(expectedExceptions={TypeCastException.class})
    public void testGetFloatFromBooleanError() {
        TypeConverter.getFloatValue(Boolean.TRUE);
    }

    @Test(expectedExceptions={TypeCastException.class})
    public void testGetFloatFromObjectError() {
        TypeConverter.getFloatValue(new Object());
    }

    @Test(expectedExceptions={TypeCastException.class})
    public void testGetFloatFromStringError() {
        TypeConverter.getFloatValue("123a");
    }


    public void testGetDoubleValue() {
        assert TypeConverter.getDoubleValue(null) == null;

        assert new Double(11.125).equals(TypeConverter.getDoubleValue(new Double(11.125)));

        assert Double.valueOf(3).equals(TypeConverter.getDoubleValue(new Integer(3)));
        assert new Double(15.2579).equals(TypeConverter.getDoubleValue("15.2579"));
        assert new Double(21.5).equals(TypeConverter.getDoubleValue(new Float(21.5)));
    }

    @Test(expectedExceptions={TypeCastException.class})
    public void testGetDoubleFromBooleanError() {
        TypeConverter.getDoubleValue(Boolean.TRUE);
    }

    @Test(expectedExceptions={TypeCastException.class})
    public void testGetDoubleFromObjectError() {
        TypeConverter.getDoubleValue(new Object());
    }

    @Test(expectedExceptions={TypeCastException.class})
    public void testGetDoubleFromStringError() {
        TypeConverter.getDoubleValue("123a");
    }


    public void testGetSQLType() {
        // Recognized types:

        assert TypeConverter.getSQLType(Boolean.TRUE) == SQLDataType.TINYINT;

        assert TypeConverter.getSQLType(new Byte((byte) 3)) == SQLDataType.TINYINT;
        assert TypeConverter.getSQLType(new Short((short) 3)) == SQLDataType.SMALLINT;
        assert TypeConverter.getSQLType(new Integer(3)) == SQLDataType.INTEGER;
        assert TypeConverter.getSQLType(new Long(3)) == SQLDataType.BIGINT;

        assert TypeConverter.getSQLType(new Float(3.0f)) == SQLDataType.FLOAT;
        assert TypeConverter.getSQLType(new Double(3.0)) == SQLDataType.DOUBLE;

        assert TypeConverter.getSQLType("three") == SQLDataType.VARCHAR;

        // Unrecognized types:

        assert TypeConverter.getSQLType(new Object()) == null;
    }
}
