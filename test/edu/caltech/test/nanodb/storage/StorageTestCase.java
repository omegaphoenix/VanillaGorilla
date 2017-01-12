package edu.caltech.test.nanodb.storage;


import java.io.File;


/**
 */
public class StorageTestCase {
    protected File testBaseDir;


    public StorageTestCase() {
        testBaseDir = new File("test_datafiles");
        if (!testBaseDir.exists())
            testBaseDir.mkdirs();
    }
}
