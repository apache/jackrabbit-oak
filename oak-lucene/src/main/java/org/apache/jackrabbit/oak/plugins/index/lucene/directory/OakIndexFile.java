/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.plugins.index.lucene.directory;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.lucene.store.DataInput;

import javax.annotation.Nonnull;
import java.io.IOException;

import static org.apache.jackrabbit.JcrConstants.JCR_DATA;
import static org.apache.jackrabbit.oak.api.Type.BINARY;

public interface OakIndexFile {
    static OakIndexFile getOakIndexFile(String name, NodeBuilder file, String dirDetails,
                                        @Nonnull BlobFactory blobFactory) {
        return getOakIndexFile(name, file, dirDetails, blobFactory, false);
    }

    static OakIndexFile getOakIndexFile(String name, NodeBuilder file, String dirDetails,
                                        @Nonnull BlobFactory blobFactory, boolean streamingWriteEnabled) {

        boolean useStreaming;
        PropertyState property = file.getProperty(JCR_DATA);
        if (property != null) { //reading
                useStreaming = property.getType() == BINARY;
        } else { //writing
            useStreaming = streamingWriteEnabled;
        }

        return useStreaming ?
                new OakStreamingIndexFile(name, file, dirDetails, blobFactory) :
                new OakBufferedIndexFile(name, file, dirDetails, blobFactory);
    }

    /**
     * @return if the file implementation supports copying data from {@link DataInput} directly.
     */
    boolean supportsCopyFromDataInput();

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

    /** Copy numBytes bytes from input to ourself. */
    void copyBytes(DataInput input, long numBytes) throws IOException;


    /**
     * Flushes the content into storage. Before calling this method, written
     * content only exist in memory
     * @throws IOException
     */
    void flush() throws IOException;
}
