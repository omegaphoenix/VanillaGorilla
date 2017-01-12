package edu.caltech.nanodb.functions;

import java.util.concurrent.ConcurrentHashMap;

import net.jcip.annotations.ThreadSafe;

/**
 * This class is a directory of all functions recognized within NanoDB,
 * including both simple and aggregate functions.
 */
@ThreadSafe
public class FunctionDirectory {

    /** The singleton instance of the function directory. */
    private static FunctionDirectory instance = new FunctionDirectory();

    /**
     * Returns the singleton instance of the function directory.
     *
     * @return the singleton instance of the function directory.
     */
    public static FunctionDirectory getInstance() {
        return instance;
    }


    private ConcurrentHashMap<String, Class<? extends Function>> functions =
        new ConcurrentHashMap<>();


    /**
     * @design (Donnie) This constructor is currently private just so that we
     *         can maintain this as a singleton.  There's no real reason why it
     *         <em>has to</em> be a singleton though, so if it makes sense at
     *         some point to make this constructor public, that's fine.
     */
    private FunctionDirectory() {
        initBuiltinFunctions();
    }


    private void initBuiltinFunctions() {
        // These are all standard (i.e. non-aggregate) functions:
        addFunction("ABS", Abs.class);
        addFunction("ACOS", ArcCos.class);
        addFunction("ASIN", ArcSin.class);
        addFunction("ATAN", ArcTan.class);
        addFunction("ATAN2", ArcTan2.class);
        addFunction("CEIL", Ceil.class);
        addFunction("CEILING", Ceil.class);
        addFunction("COALESCE", Coalesce.class);
        addFunction("CONCAT", Concat.class);
        addFunction("COS", Cos.class);
        addFunction("FLOOR", Floor.class);
        addFunction("GREATEST", Greatest.class);
        addFunction("IF", If.class);
        addFunction("IFNULL", IfNull.class);
        addFunction("LEAST", Least.class);
        addFunction("NULLIF", NullIf.class);
        addFunction("POW", Pow.class);
        addFunction("POWER", Pow.class);
        addFunction("ROUND", Round.class);
        addFunction("SIN", Sin.class);
        addFunction("SQRT", Sqrt.class);
        addFunction("TAN", Tan.class);

        addFunction("PERF_COUNTER", ReadPerfCounter.class);
        addFunction("RESET_PERF_COUNTER", ResetPerfCounter.class);

        // These are the aggregate functions:
        addFunction("AVG", Avg.class);
        addFunction("AVG#DISTINCT", AvgDistinct.class);
        addFunction("COUNT", Count.class);
        addFunction("COUNT#DISTINCT", CountDistinct.class);
        addFunction("COUNT#STAR", CountStar.class);
        addFunction("MAX", Max.class);
        addFunction("MIN", Min.class);
        addFunction("SUM", Sum.class);
        addFunction("SUM#DISTINCT", SumDistinct.class);
        addFunction("STDDEV", StdDev.class);
        addFunction("VARIANCE", Variance.class);
    }


    /**
     * Add a function to the directory.  The function's name is trimmed and
     * converted to uppercase before updating the directory.  If the directory
     * already contains a function with the specified name, an exception is
     * reported.
     *
     * @param funcName the name of the function
     * @param impl the {@link Function} object that implements this function
     *
     * @throws IllegalArgumentException if the name or implementation is
     *         <tt>null</tt>, or if the function already appears in the
     *         directory
     */
    public void addFunction(String funcName, Class<? extends Function> impl) {
        if (funcName == null)
            throw new IllegalArgumentException("funcName cannot be null");

        if (impl == null)
            throw new IllegalArgumentException("impl cannot be null");

        if (!Function.class.isAssignableFrom(impl)) {
            throw new IllegalArgumentException(
                "impl must represent a subclass of Function; got " + impl.getName());
        }

        // Probably, function names will come in cleaned up, but this will make
        // doubly sure!
        funcName = funcName.trim().toUpperCase();
        if (functions.containsKey(funcName)) {
            throw new IllegalArgumentException("Function " + funcName +
                " is already in the directory");
        }

        functions.put(funcName, impl);
    }


    /**
     * If the specified function name corresponds to an existing function,
     * this method instantiates a new object that can compute the function,
     * and then returns it.
     *
     * @param funcName the name of the function to get
     *
     * @return an object that can compute the function
     */
    public Function getFunction(String funcName) {
        funcName = funcName.trim().toUpperCase();
        Class<? extends Function> impl = functions.get(funcName);
        if (impl == null) {
            throw new UnsupportedOperationException("No function named " +
                funcName);
        }

        try {
            return impl.getConstructor().newInstance();
        }
        catch (Exception e) {
            throw new RuntimeException("Couldn't instantiate function " +
                                       funcName, e);
        }
    }
}
