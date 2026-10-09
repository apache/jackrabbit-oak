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
package org.apache.jackrabbit.oak.plugins.index.luceneNg.directory;

import java.io.IOException;

/**
 * Abstraction for reading and writing index files stored in Oak.
 * Handles chunking and buffering of file data.
 * Adapted from oak-lucene for Lucene 9.
 */
public interface OakIndexFile {

    /**
     * @return name of the index file
     */
    String getName();

    /**
     * @return length of index file in bytes
     */
    long length();

    /**
     * @return true if the file has been closed
     */
    boolean isClosed();

    /**
     * Close the file, releasing any resources.
     */
    void close();

    /**
     * @return current position within the file
     */
    long position();

    /**
     * Seek to a specific position in the file.
     *
     * @param pos the position to seek to
     * @throws IOException if seek fails
     */
    void seek(long pos) throws IOException;

    /**
     * Create a clone of this file for concurrent access.
     *
     * @return cloned instance
     */
    OakIndexFile clone();

    /**
     * Read bytes from the file into the given array.
     *
     * @param b      byte array to read into
     * @param offset offset in the array to start writing
     * @param len    number of bytes to read
     * @throws IOException if read fails
     */
    void readBytes(byte[] b, int offset, int len) throws IOException;

    /**
     * Write bytes from the given array into the file.
     *
     * @param b      byte array to write from
     * @param offset offset in the array to start reading
     * @param len    number of bytes to write
     * @throws IOException if write fails
     */
    void writeBytes(byte[] b, int offset, int len) throws IOException;

    /**
     * Flush any buffered writes to storage.
     *
     * @throws IOException if flush fails
     */
    void flush() throws IOException;
}
