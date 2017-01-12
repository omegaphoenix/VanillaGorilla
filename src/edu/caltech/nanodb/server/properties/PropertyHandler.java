package edu.caltech.nanodb.server.properties;


import edu.caltech.nanodb.expressions.TypeCastException;


/**
 * <p>
 * This interface can be implemented by components of the database that want
 * to expose properties that can be viewed or manipulated by the NanoDB client
 * via the <tt>SHOW VARIABLES</tt> or <tt>SET <em>varname</em> = ...</tt>
 * SQL commands.  The interface allows multiple properties to be handled by a
 * single component by requiring the property name to be specified when
 * getting or setting the property value.
 * </p>
 * <p>
 * Components that want to expose one or more properties must implement this
 * interface, and then register the specific properties on the
 * {@code PropertyRegistry} by calling
 * {@link PropertyRegistry#registerProperties} during initialization.
 * </p>
 */
public interface PropertyHandler {

    /**
     * Retrieves the value of the specified property.
     *
     * @param propertyName the name of the property to retrieve
     *
     * @return the current value of the property
     */
    Object getPropertyValue(String propertyName)
        throws UnrecognizedPropertyException;

    /**
     * Sets the value of the specified property.
     *
     * @param propertyName the name of the property to set
     *
     * @param value the new value to set the property to
     *
     * @throws ReadOnlyPropertyException if the specified property is
     *         read-only and cannot be modified.
     *
     * @throws TypeCastException if the specified value is not the correct
     *         type for the property, and cannot be coerced into the correct
     *         type by the {@link edu.caltech.nanodb.expressions.TypeConverter}.
     */
    void setPropertyValue(String propertyName, Object value)
        throws UnrecognizedPropertyException, ReadOnlyPropertyException,
               TypeCastException;
}
