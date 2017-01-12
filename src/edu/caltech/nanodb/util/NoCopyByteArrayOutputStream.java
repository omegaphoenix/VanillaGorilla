package edu.caltech.nanodb.util;


import java.io.ByteArrayOutputStream;


/**
 * This class is a simple extension of Java's {@link ByteArrayOutputStream} that
 * exposes the internal buffer without having to make a copy of it via the
 * normal {@link ByteArrayOutputStream#toByteArray()} method.
 */
public class NoCopyByteArrayOutputStream extends ByteArrayOutputStream {
    /**
     * Returns a reference to the internal byte-buffer used by this byte-array
     * output stream.
     *
     * @return a reference to the internal byte-buffer used by this byte-array
     *         output stream.
     */
    public byte[] getBuf() {
        return buf;
    }
}
