package edu.caltech.nanodb.queryeval;


import edu.caltech.nanodb.expressions.TypeCastException;
import edu.caltech.nanodb.server.properties.PropertyHandler;
import edu.caltech.nanodb.server.properties.ReadOnlyPropertyException;
import edu.caltech.nanodb.server.properties.UnrecognizedPropertyException;
import edu.caltech.nanodb.storage.StorageManager;


/**
 * This class allows various kinds of query planners (subclasses of
 * {@link Planner}) to be instantiated for use in query evaluation.
 */
public class PlannerFactory {
    /**
     * This property can be used to specify a different query-planner class
     * for NanoDB to use.
     */
    public static final String PROP_PLANNER_CLASS = "nanodb.planner.class";


    /**
     * This class is the default planner used in NanoDB, unless
     * overridden in the configuration.
     */
    public static final String DEFAULT_PLANNER =
        "edu.caltech.nanodb.queryeval.CostBasedJoinPlanner";


    public static class PlannerFactoryPropertyHandler implements PropertyHandler {

        @Override
        public Object getPropertyValue(String propertyName)
                throws UnrecognizedPropertyException {

            if (PROP_PLANNER_CLASS.equals(propertyName)) {
                return getPlannerClass();
            }
            else {
                throw new UnrecognizedPropertyException("No property named " +
                        propertyName);
            }
        }

        @Override
        public void setPropertyValue(String propertyName, Object value)
                throws UnrecognizedPropertyException, ReadOnlyPropertyException,
                TypeCastException {

            if (PROP_PLANNER_CLASS.equals(propertyName)) {
                setPlannerClass(value.toString());
            }
            else {
                throw new UnrecognizedPropertyException("No property named " +
                        propertyName);
            }
        }
    }




    private PlannerFactory() {
        throw new UnsupportedOperationException(
            "This class should not be instantiated.");
    }


    public static String getPlannerClass() {
        return System.getProperty(PROP_PLANNER_CLASS, DEFAULT_PLANNER);
    }


    public static void setPlannerClass(String className) {

        // Make sure the specified class name is actually a planner class!
        try {
            // Pass null for the storage manager since we are simply seeing
            // if we can instantiate the class at all.
            getPlanner(className, null);
        }
        catch (Exception e) {
            throw new RuntimeException("Value \"" + className +
                "\" doesn't correspond to a Planner class");
        }

        // If that succeeded, we can set the property value.
        System.setProperty(PROP_PLANNER_CLASS, className);
    }


    public static Planner getPlanner(StorageManager storageManager) {
        return getPlanner(getPlannerClass(), storageManager);
    }


    private static Planner getPlanner(String className,
                                      StorageManager storageManager) {
        try {
            // Load and instantiate the specified planner class.
            Class c = Class.forName(className);
            Planner p = (Planner) c.newInstance();
            p.setStorageManager(storageManager);
            return p;
        }
        catch (Exception e) {
            throw new RuntimeException(
                "Couldn't instantiate Planner class " + className, e);
        }
    }
}

