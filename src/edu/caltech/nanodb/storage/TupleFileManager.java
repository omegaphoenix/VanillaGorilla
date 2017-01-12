package edu.caltech.nanodb.storage;


import java.io.IOException;

import edu.caltech.nanodb.relations.TableSchema;


/**
 * This interface defines the operations that can be performed on
 * {@link TupleFile}s, but that are at a higher level of implementation than
 * the tuple file itself.  Examples of such operations are creating a new
 * tuple file on disk, deleting a tuple file from the disk, and so forth.
 */
public interface TupleFileManager {

    public TupleFile createTupleFile(DBFile dbFile, TableSchema schema)
        throws IOException;


    public TupleFile openTupleFile(DBFile dbFile) throws IOException;


    public void saveMetadata(TupleFile tupleFile) throws IOException;


    public void deleteTupleFile(TupleFile tupleFile) throws IOException;
}
