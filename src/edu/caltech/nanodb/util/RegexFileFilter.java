package edu.caltech.nanodb.util;


import java.io.File;
import java.io.FileFilter;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


/**
 * This class provides a simple regex-based file filter for use with
 * {@link File#listFiles(java.io.FileFilter)}.  The regex pattern is provided
 * to the constructor and is used to check files when retrieving directory
 * listings.  Note that only the file's name is tested; the path is not
 * considered in the check.
 * 
 * @see File#listFiles(java.io.FileFilter)
 * @see Pattern
 */
public class RegexFileFilter implements FileFilter {

    /** The regex pattern used for matching against filenames. */
    private Pattern pattern;


    /**
     * This matcher is initialized from the regex pattern, and is used to do
     * the actual filename testing.
     */
    private Matcher matcher;


    /**
     * Initialize a new regex file filter with the specified regex pattern.
     *
     * @param regex The regex pattern to check filenames against.
     */
    public RegexFileFilter(String regex) {
        pattern = Pattern.compile(regex);
        matcher = pattern.matcher("");
    }


    @Override
    public boolean accept(File file) {
        matcher.reset(file.getName());
        return matcher.matches();
    }
}
