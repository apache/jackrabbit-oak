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
package org.apache.jackrabbit.oak.spi.blob;

import java.io.IOException;
import java.io.InputStream;

/**
 * An interface to store and read large binary objects.
 */
public interface BlobStore {

    /**
     * Write a blob from an input stream.
     * This method closes the input stream.
     *
     * @param in the input stream
     * @return the blob id
     */
    String writeBlob(InputStream in) throws IOException;

    /**
     * Read a number of bytes from a blob.
     * 
     * @param blobId the blob id
     * @param pos the position within the blob
     * @param buff the target byte array
     * @param off the offset within the target array
     * @param length the number of bytes to read
     * @return the number of bytes read
     */
    int readBlob(String blobId, long pos, byte[] buff, int off, int length) throws IOException;

    /**
     * Get the length of the blob.
     * 
     * @param blobId the blob id
     * @return the length
     */
    long getBlobLength(String blobId) throws IOException;

    /**
     * Returns a new stream for given blobId. The streams returned from
     * multiple calls to this method are byte wise equals. That is,
     * subsequent calls to {@link java.io.InputStream#read() read}
     * return the same sequence of bytes as long as neither call throws
     * an exception.
     *
     * @param blobId the blob id
     * @return a new stream for given blobId
     */
    InputStream getInputStream(String blobId) throws IOException;

}
