package edu.caltech.nanodb.server.properties;

import edu.caltech.nanodb.expressions.TypeCastException;
import org.apache.log4j.Logger;

import java.util.Collections;
import java.util.HashMap;
import java.util.Set;


/**
 * This is the central location where all properties exposed by the database
 * can be registered and accessed.  Various components register their
 * configurable properties with the registry, and then they can be accessed
 * and/or set from the SQL prompt.
 */
public class PropertyRegistry {

    /** A logging object for reporting anything interesting that happens. */
    private static Logger logger = Logger.getLogger(PropertyRegistry.class);

    /** This is the singleton instance of the property registry. */
    @Deprecated
    private static PropertyRegistry instance = new PropertyRegistry();


    /**
     * Returns the singleton instance of the property registry.
     *
     * @return the singleton instance of the property registry
     */
    @Deprecated
    public static PropertyRegistry getInstance() {
        return instance;
    }


    private HashMap<String, PropertyHandler> properties = new HashMap<>();


    public void registerProperties(PropertyHandler handler,
                                   String... propertyNames) {
        for (String name : propertyNames) {
            if (properties.containsKey(name)) {
                throw new IllegalArgumentException("Property name \"" + name +
                    "\" is already registered.");
            }
            properties.put(name, handler);
        }
    }


    public void unregisterProperties(String... propertyNames) {
        for (String name : propertyNames)
            properties.remove(name);
    }


    public void unregisterAllProperties() {
        properties.clear();
    }


    public Set<String> getAllPropertyNames() {
        return Collections.unmodifiableSet(properties.keySet());
    }


    public Object getPropertyValue(String propertyName)
        throws UnrecognizedPropertyException {

        if (propertyName == null)
            throw new IllegalArgumentException("propertyName cannot be null");

        PropertyHandler handler = properties.get(propertyName);
        if (handler == null) {
            throw new UnrecognizedPropertyException("No property named \"" +
                propertyName + "\"");
        }

        return handler.getPropertyValue(propertyName);
    }


    public void setPropertyValue(String propertyName, Object value)
        throws UnrecognizedPropertyException, ReadOnlyPropertyException,
               TypeCastException {

        if (propertyName == null)
            throw new IllegalArgumentException("propertyName cannot be null");

        PropertyHandler handler = properties.get(propertyName);
        if (handler == null) {
            throw new UnrecognizedPropertyException("No property named \"" +
                    propertyName + "\"");
        }

        handler.setPropertyValue(propertyName, value);
    }
}
