package edu.caltech.nanodb.commands;

import java.util.Collections;
import java.util.HashMap;
import java.util.Set;

import edu.caltech.nanodb.expressions.TypeConverter;


/**
 * This class holds properties that might contain additional details for a
 * command.  For example, the <tt>CREATE TABLE</tt> DDL command takes
 * properties to specify non-default storage engine types, the page size to
 * use for the table file, and so forth.
 */
public class CommandProperties {
    /** A map of name-value pairs that represent the actual properties. */
    private HashMap<String, Object> values = new HashMap<>();


    public void set(String name, Object value) {
        if (name == null)
            throw new IllegalArgumentException("name cannot be null");

        if (value == null)
            throw new IllegalArgumentException("value cannot be null");

        values.put(name, value);
    }


    public Object get(String name, Object defaultValue) {
        Object value = values.get(name);
        if (value == null)
            value = defaultValue;

        return value;
    }


    public Object get(String name) {
        return get(name, null);
    }


    public int getInt(String name, int defaultValue) {
        Object obj = get(name);
        if (obj == null)
            return defaultValue;

        Integer intObj = TypeConverter.getIntegerValue(obj);
        return intObj.intValue();
    }


    public String getString(String name, String defaultValue) {
        Object obj = get(name);
        if (obj == null)
            return defaultValue;

        return obj.toString();
    }


    public Set<String> getNames() {
        return Collections.unmodifiableSet(values.keySet());
    }


    @Override
    public String toString() {
        return "CommandProperties[" + values.toString() + "]";
    }
}
