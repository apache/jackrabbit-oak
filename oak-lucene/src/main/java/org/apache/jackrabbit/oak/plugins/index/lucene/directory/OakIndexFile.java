package org.apache.jackrabbit.oak.plugins.index.lucene.directory;

import java.io.IOException;

public interface OakIndexFile {
    /**
     * @return name of the index being accessed
     */
    String getName();

    /**
     * @return length of index file
     */
    long length();

    boolean isClosed();

    void close();

    /**
     * @return current location of access
     */
    long position();

    /**
     * Seek current location of access to {@code pos}
     * @param pos
     * @throws IOException
     */
    void seek(long pos) throws IOException;

    /**
     * Duplicates this instance to be used by a different consumer/thread.
     * State of the cloned instance is same as original. Once cloned, the states
     * would change separately according to how are they accessed.
     *
     * @return cloned instance
     */
    OakIndexFile clone();

    /**
     * Read {@code len} number of bytes from underlying storage and copy them
     * into byte array {@code b} starting at {@code offset}
     * @param b byte array to copy contents read from storage
     * @param offset index into {@code b} where the copy begins
     * @param len numeber of bytes to be read from storage
     * @throws IOException
     */
    void readBytes(byte[] b, int offset, int len)
            throws IOException;

    /**
     * Writes {@code len} number of bytes from byte array {@code b}
     * starting at {@code offset} into the underlying storage
     * @param b byte array to copy contents into the storage
     * @param offset index into {@code b} where the copy begins
     * @param len numeber of bytes to be written to storage
     * @throws IOException
     */
    void writeBytes(byte[] b, int offset, int len)
            throws IOException;

    /**
     * Flushes the content into storage. Before calling this method, written
     * content only exist in memory
     * @throws IOException
     */
    void flush() throws IOException;
}
