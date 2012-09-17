/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.mongomk.api;

import java.io.InputStream;

import org.apache.jackrabbit.mk.api.MicroKernel;

/**
 * The <code>BlobStore</code> interface deals with all blob related operations of the {@link MicroKernel}.
 *
 * <p>
 * Since binary storage and node storage most likely use different backend technologies two separate interfaces for
 * these operations are provided.
 * </p>
 *
 * <p>
 * This interface is not only a partly {@code MicroKernel} but also provides a different layer of abstraction by
 * converting the {@link String} parameters into higher level objects to ease the development for implementors of the
 * {@code MicroKernel}.
 * </p>
 *
 * @see NodeStore
 *
 * @author <a href="mailto:pmarx@adobe.com>Philipp Marx</a>
 */
public interface BlobStore {

    /**
     * @see MicroKernel#getLength(String)
     *
     * @param blobId The id of the blob.
     * @return The length in bytes.
     * @throws Exception If an error occurred while getting the blob lenght.
     */
    long getBlobLength(String blobId) throws Exception;

    /**
     * @see MicroKernel#read(String, long, byte[], int, int)
     *
     * @param blobId The id of the blob.
     * @param blobOffset The offset to read from.
     * @param buffer The buffer to read the binary data into.
     * @param bufferOffset The offset to read into the buffer.
     * @param length The length of the data to read.
     * @return The actual number of bytes which were read.
     * @throws Exception If an error occurred while reading the blob data.
     */
    int readBlob(String blobId, long blobOffset, byte[] buffer, int bufferOffset, int length) throws Exception;

    /**
     * @see MicroKernel#write(InputStream)
     *
     * @param is The {@link InputStream} containing the data which should be written.
     * @return The id of the blob.
     * @throws Exception If an error occurred while writing the data.
     */
    String writeBlob(InputStream is) throws Exception;
}
