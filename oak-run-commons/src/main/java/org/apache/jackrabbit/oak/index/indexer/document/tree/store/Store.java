/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.jackrabbit.oak.index.indexer.document.tree.store;

import java.util.Properties;
import java.util.Set;

/**
 * Storage for files in a tree store.
 */
public interface Store {

    public static final String MAX_FILE_SIZE_BYTES = "maxFileSizeBytes";
    public static final int DEFAULT_MAX_FILE_SIZE_BYTES = 8 * 1024 * 1024;

    /**
     * Get a file
     *
     * @param key the file name
     * @return the file
     */
    default PageFile get(String key) {
        PageFile result = getIfExists(key);
        if (result == null) {
            throw new IllegalStateException("Not found: " + key);
        }
        return result;
    }

    /**
     * Get a file if it exists
     *
     * @param key the file name
     * @return the file, or null
     */
    PageFile getIfExists(String key);

    /**
     * Storage a file.
     *
     * @param key the file name
     * @param value the file
     */
    void put(String key, PageFile value);

    /**
     * Generate a new file name.
     *
     * @return
     */
    String newFileName();

    /**
     * Get the list of files.
     *
     * @return the result
     */
    Set<String> keySet();

    /**
     * Remove a number of files.
     *
     * @param set the result
     */
    void remove(Set<String> set);

    /**
     * Remove all files.
     */
    void removeAll();

    /**
     * Get the number of files written.
     *
     * @return the result
     */
    long getWriteCount();

    /**
     * Get the number of files read.
     *
     * @return the result
     */
    long getReadCount();

    /**
     * Set the compression algorithm used for writing from now on.
     *
     * @param compression the compression algorithm
     */
    void setWriteCompression(Compression compression);

    /**
     * Close the store
     */
    void close();

    Properties getConfig();

    /**
     * Get the maximum file size configured.
     *
     * @return the file size, in bytes
     */
    long getMaxFileSizeBytes();

    default boolean supportsByteOperations() {
        return false;
    }

    default byte[] getBytes(String key) {
        throw new UnsupportedOperationException();
    }

    default void putBytes(String key, byte[] data) {
        throw new UnsupportedOperationException();
    }

}
