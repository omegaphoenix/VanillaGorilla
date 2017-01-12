package edu.caltech.test.nanodb.storage;

import java.io.IOException;

import org.testng.annotations.Test;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.AfterClass;

import edu.caltech.nanodb.storage.*;

/**
 * This test class exercises the functionality of the
 * {@link edu.caltech.nanodb.storage.DBPage} class.
 **/
@Test
public class TestDBPage extends StorageTestCase {

    /** This is the filename used for the tests in this class. */
    private final String TEST_FILE_NAME = "TestDBPage_TestFile";


    /** This is the file-manager instance used for the tests in this class. */
    private FileManager fileMgr;


    /** This is the buffer-manager instance used for tests in this class. */
    private BufferManager bufMgr;


    /**
     * Instances of <tt>DBPage</tt> must be associated with a <tt>DBFile</tt>,
     * so this is the file used for testing.  It is created and cleaned up by
     * this class.
     */
    private DBFile dbFile;


    /**
     * This is the <tt>DBPage</tt> object that all tests run against.  Since we
     * are simply writing various values to the data page, we can use a single
     * object for all the different tests.
     */
    private DBPage dbPage;


    /**
     * This set-up method initializes the file manager, data-file, and page that
     * all tests will run against.
     */
    @BeforeClass
    public void beforeClass() throws IOException {

        fileMgr = new FileManagerImpl(testBaseDir);
        bufMgr = new BufferManager(null, fileMgr);

        // Get DBFile
        DBFileType type = DBFileType.HEAP_TUPLE_FILE;

        try {
             int pageSize = DBFile.DEFAULT_PAGESIZE; // 8k
             dbFile = fileMgr.createDBFile(TEST_FILE_NAME, type, pageSize);
        }
        catch (IOException e) {
            // The file is already created
        }

        dbPage = new DBPage(bufMgr, dbFile, 0);
    }


	/**
	 * Remove the dbFile created in beforeClass().
	 *
	 * @throws IOException
	 */
	@AfterClass
	public void afterClass() throws IOException {
		fileMgr.deleteDBFile(dbFile);
	}


	/**
	 * Test readBoolean() and writeBoolean() methods in DBPage
	 */
	@Test
	public void testReadWriteBoolean() {

		// Write two canary bytes at position1 and position2
		int position1 = 0;
		int positionValue = 1;	// a boolean is 1 byte long.
		int position2 = 3;
		int canary1 = 0xCC;		// = 204
		int canary2 = 0xAA;		// = 170
		dbPage.writeByte(position1, canary1);
		dbPage.writeByte(position2, canary2);

		// write Boolean value at position 1
		dbPage.writeBoolean(positionValue, true);
		assert dbPage.readBoolean(positionValue);

		// Read back canary values.
		assert (dbPage.readByte(position1) == (byte) canary1);
		assert (dbPage.readByte(position2) == (byte) canary2);
	}


	/**
	 * Test readShort(), readUnsignedShort(), and writeShort() methods
	 * in DBPage.
	 */
	@Test
	public void testReadWriteShort() {

		// Write two canary bytes at position1 and position2
		int position1 = 0;
		int positionValue = 1;	// a short is 2 bytes long.
		int position2 = 3;
		int canary1 = 0xCC;		// = 204
		int canary2 = 0xAA;		// = 170
		dbPage.writeByte(position1, canary1);
		dbPage.writeByte(position2, canary2);

		// Test the largest short 65535
		int value = 65535;
		dbPage.writeShort(positionValue, value);
		assert (dbPage.readShort(positionValue) == (short) value);
		assert (dbPage.readUnsignedShort(positionValue) == value);

		// Test negative short (-3) = 1111 1101
		value = (-3);
		dbPage.writeShort(positionValue, value);
		assert (dbPage.readShort(positionValue) == (short) value);

		// Read back canary values.
		assert (dbPage.readByte(position1) == (byte) canary1);
		assert (dbPage.readByte(position2) == (byte) canary2);
	}


	/**
	 * Test readInt(), readUnsignedInt(), and writeInt() methods
	 * in DBPage.
	 */
	@Test
	public void testReadWriteInt() {

		// Write two canary bytes at position1 and position2
		int position1 = 0;
		int positionValue = 1;	// an int is 4 bytes long.
		int position2 = 5;
		int canary1 = 0xCC;		// = 204
		int canary2 = 0xAA;		// = 170
		dbPage.writeByte(position1, canary1);
		dbPage.writeByte(position2, canary2);

		// Test the largest int 2147483647
		int value = 2147483647;
		dbPage.writeInt(positionValue, value);
		assert (dbPage.readInt(positionValue) == value);
		assert (dbPage.readUnsignedInt(positionValue) == (long) value);

		// Test negative int (-3) = 1111 1111 1111 1101
		value = (-3);
		dbPage.writeInt(positionValue, value);
		assert (dbPage.readInt(positionValue) == value);

		// Read back canary values.
		assert (dbPage.readByte(position1) == (byte) canary1);
		assert (dbPage.readByte(position2) == (byte) canary2);
	}


	/**
	 * Test readLong() and writeLong() methods in DBPage.
	 */
	@Test
	public void testReadWriteLong() {

		// Write two canary bytes at position1 and position2
		int position1 = 0;
		int positionValue = 1;	// Long is 8 bytes.
		int position2 = 9;
		int canary1 = 0xCC;		// = 204
		int canary2 = 0xAA;		// = 170
		dbPage.writeByte(position1, canary1);
		dbPage.writeByte(position2, canary2);

		// Test the large long 9223372036854775807
		long value = 9223372036854775807L;
		dbPage.writeLong(positionValue, value);
		assert (dbPage.readLong(positionValue) == value);

		// Test negative long (-3) = 1...   1111 1101
		value = -3L;
		dbPage.writeLong(positionValue, value);
		assert (dbPage.readLong(positionValue) == value);

		// Read back canary values.
		assert (dbPage.readByte(position1) == (byte) canary1);
		assert (dbPage.readByte(position2) == (byte) canary2);
	}


	/**
	 * Test readFloat() and writeFloat() methods in DBPage.
	 */
	@Test
	public void testReadWriteFloat() {

		// Write two canary bytes at position1 and position2
		int position1 = 0;
		int positionValue = 1;	// float is 4 bytes.
		int position2 = 5;
		int canary1 = 0xCC;		// = 204
		int canary2 = 0xAA;		// = 170
		dbPage.writeByte(position1, canary1);
		dbPage.writeByte(position2, canary2);

		// Test the largest float 2147483647
		float value = 2147483647F;
		dbPage.writeFloat(positionValue, value);
		assert (dbPage.readFloat(positionValue) == value);

		// Test float with decimal point: 21474.83645
		value = 21474.83645F;
		dbPage.writeFloat(positionValue, value);
		assert (dbPage.readFloat(positionValue) == value);

		// Test negative float (-3)
		value = -3F;
		dbPage.writeFloat(positionValue, value);
		assert (dbPage.readFloat(positionValue) == value);

		// Test negative float with decimal point: (-3.25)
		value = -3.25F;
		dbPage.writeFloat(positionValue, value);
		assert (dbPage.readFloat(positionValue) == value);

		// Read back canary values.
		assert (dbPage.readByte(position1) == (byte) canary1);
		assert (dbPage.readByte(position2) == (byte) canary2);
	}

	/**
	 * Test readDouble() and writeDouble() methods in DBPage.
	 */
	@Test
	public void testReadWriteDouble() {

		// Write two canary bytes at position1 and position2
		int position1 = 0;
		int positionValue = 1;	// Double is 8 bytes.
		int position2 = 9;
		int canary1 = 0xCC;		// = 204
		int canary2 = 0xAA;		// = 170
		dbPage.writeByte(position1, canary1);
		dbPage.writeByte(position2, canary2);

		// Test the largest double 9223372036854775807
		double value = 9223372036854775807D;
		dbPage.writeDouble(positionValue, value);
		assert (dbPage.readDouble(positionValue) == value);

		// Test double with decimal point: 92233720.36854775805
		value = 92233720.36854775805D;
		dbPage.writeDouble(positionValue, value);
		assert (dbPage.readDouble(positionValue) == value);

		// Test negative double (-3) = 1...   1111 1101
		value = -3D;
		dbPage.writeDouble(positionValue, value);
		assert (dbPage.readDouble(positionValue) == value);

		// Test negative double with decimal point: (-3.25)
		value = -3.25D;
		dbPage.writeDouble(positionValue, value);
		assert (dbPage.readDouble(positionValue) == value);

		// Read back canary values.
		assert (dbPage.readByte(position1) == (byte) canary1);
		assert (dbPage.readByte(position2) == (byte) canary2);
	}


	/**
	 * Test readChar() and writeChar() methods in DBPage.
	 */
	@Test
	public void testReadWriteChar() {

		// Write two canary bytes at position1 and position2
		int position1 = 0;
		int positionValue = 1;	// a char is 2 bytes long
		int position2 = 3;
		int canary1 = 0xCC;		// = 204
		int canary2 = 0xAA;		// = 170
		dbPage.writeByte(position1, canary1);
		dbPage.writeByte(position2, canary2);

		// Test a char in ASCII: '@' = 64 in decimal representation
		char value = '@';
		dbPage.writeChar(positionValue, value);
		assert (dbPage.readChar(positionValue) == value);

		// Test a char in ASCII: '~' = 126 in decimal representation
		value = '~';
		dbPage.writeChar(positionValue, value);
		assert (dbPage.readChar(positionValue) == value);

		// Read back canary values.
		assert (dbPage.readByte(position1) == (byte) canary1);
		assert (dbPage.readByte(position2) == (byte) canary2);
	}


	/**
	 * Test readFixedSizeString() and writeFixedSizeString() methods
	 * in DBPage.
	 */
	@Test
	public void testReadWriteFixedSizeString() {

		// Write two canary bytes at position1 and position2
		int position1 = 0;
		int positionValue = 1;
		int strlength = 18;		// We test string of length 18
		int position2 = 19;
		int canary1 = 0xCC;		// = 204
		int canary2 = 0xAA;		// = 170
		dbPage.writeByte(position1, canary1);
		dbPage.writeByte(position2, canary2);

		// Test a string of length exactly 18
		String value = "This is a string.";
		dbPage.writeFixedSizeString(positionValue, value, strlength);
		assert value.equals(dbPage.readFixedSizeString(positionValue, strlength));

		// Test a string of length less than 18
		value = "This is a ";
		dbPage.writeFixedSizeString(positionValue, value, strlength);
		assert value.equals(dbPage.readFixedSizeString(positionValue, strlength));

		// Read back canary values.
		assert (dbPage.readByte(position1) == (byte) canary1);
		assert (dbPage.readByte(position2) == (byte) canary2);
	}


	/**
	 * Test readVarString255() and writeVarString255() methods
	 * in DBPage.
	 */
	@Test
	public void testReadWriteVarString255() {

		// Write two canary bytes at position1 and position2
		int position1 = 0;
		int positionValue = 1;	// The total length is 255 + 1, the length
		int position2 = 257;
		int canary1 = 0xCC;		// = 204
		int canary2 = 0xAA;		// = 170
		dbPage.writeByte(position1, canary1);
		dbPage.writeByte(position2, canary2);

		// Test a string of length shorter than 255
		String value = "This is a string.";
		dbPage.writeVarString255(positionValue, value);
		assert value.equals(dbPage.readVarString255(positionValue));

		// Test a string of length exactly 255
		value = "A SURGE in the radiation levels surrounding the " +
			"reactors at the Dai-ichi nuclear power plant at Fukushima " +
			"on Wednesday morning forced authorities to withdraw workers " +
			"from the site of Japan's escalating nuclear catastrophe. " +
			"A skeleton crew of 50, these are";
        assert value.length() == 255;  // Sanity check.

		dbPage.writeVarString255(positionValue, value);
		//System.out.println(value);
		//System.out.println(dbPage.readVarString255(positionValue));
		assert value.equals(dbPage.readVarString255(positionValue));

		// Read back canary values.
		assert (dbPage.readByte(position1) == (byte) canary1);
		assert (dbPage.readByte(position2) == (byte) canary2);
	}


	/**
	 * Test readVarString65535() and writeVarString65535() methods
	 * in DBPage.
	 */
	@Test
	public void testReadWriteVarString65535() {

		// Write two canary bytes at position1 and position2
		int position1 = 0;
		int positionValue = 1;	// The total length is 8188 + 2, the length
		int position2 = 8191;
		int canary1 = 0xCC;		// = 204
		int canary2 = 0xAA;		// = 170
		dbPage.writeByte(position1, canary1);
		dbPage.writeByte(position2, canary2);

		// Test a string of length shorter than page size
		// We do not check canary values for this write.
		String value = "This is a string.";
		dbPage.writeVarString65535(positionValue, value);
		assert value.equals(dbPage.readVarString65535(positionValue));

		// Test a string of length that fills the page size
		// repeat the string 32 times to get 8160 characters
		value = "";
		for (int i = 0; i < 32; i++) {
		value += "A SURGE in the radiation levels surrounding the " +
			"reactors at the Dai-ichi nuclear power plant at Fukushima " +
			"on Wednesday morning forced authorities to withdraw workers " +
			"from the site of Japan's escalating nuclear catastrophe. " +
			"A skeleton crew of 50, these are";
		}

		// add 28 characters to be 8188
		value += "A SURGE in the radiation lev";
		assert value.length() == 8188 : "test string length is not 8188 (it's "
			+ value.length() + ")";

		// Test write and read.
		dbPage.writeVarString65535(positionValue, value);
		assert value.equals(dbPage.readVarString65535(positionValue));

		// Read back canary values.
		assert (dbPage.readByte(position1) == (byte) canary1);
		assert (dbPage.readByte(position2) == (byte) canary2);
	}

}
