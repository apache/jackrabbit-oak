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

import org.osgi.annotation.versioning.ProviderType;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

/**
 * A blob store that supports garbage collection.
 */
@ProviderType
public interface GarbageCollectableBlobStore extends BlobStore {

    /**
     * Set the block size used by this blob store, if the blob store splits
     * binaries into blocks. If not, this setting is ignored.
     * 
     * @param x the block size in bytes.
     */
    void setBlockSize(int x);

    /**
     * Write a blob from a temporary file. The temporary file is removed
     * afterwards. A file based blob stores might simply rename the file, so
     * that no additional writes are necessary.
     *
     * @param tempFileName the temporary file name
     * @return the blob id
     */
    String writeBlob(String tempFileName) throws IOException;

    /**
     * Remove all unused blocks.
     * 
     * @return the number of removed blocks
     */
    int sweep() throws IOException;

    /**
     * Start the mark phase.
     */
    void startMark() throws IOException;

    /**
     * Clear all objects marked as "transiently in use".
     */
    void clearInUse();

    /**
     * Clear the cache.
     */
    void clearCache();

    /**
     * Get the minimum block size (if there is any).
     * 
     * @return the block size
     */
    long getBlockSizeMin();

    /**
     * Gets all the identifiers.
     * 
     * @param maxLastModifiedTime
     *            the max last modified time to consider for retrieval,
     *            with the special value '0' meaning no filtering by time
     * @return the identifiers
     * @throws Exception
     *             the exception
     */
    Iterator<String> getAllChunkIds(long maxLastModifiedTime) throws Exception;

    /**
     * Deletes the blobs with the given ids.
     *
     * @param chunkIds the chunk ids
     * @param maxLastModifiedTime the max last modified time to consider for retrieval,
     *            with the special value '0' meaning no filtering by time
     * @return true, if successful
     * @throws Exception the exception
     */
    @Deprecated
    boolean deleteChunks(List<String> chunkIds, long maxLastModifiedTime) throws Exception;

    /**
     * Deletes the blobs with the given ids.
     *
     * @param chunkIds the chunk ids
     * @param maxLastModifiedTime the max last modified time to consider for retrieval,
     *            with the special value '0' meaning no filtering by time
     * @return long the count of successful deletions
     * @throws Exception the exception
     */
    long countDeleteChunks(List<String> chunkIds, long maxLastModifiedTime) throws Exception;

    /**
     * Resolve chunks stored in the blob store from the given Id.
     * This will not return any chunks stored in-line in the id.
     * 
     * @param blobId the blob id
     * @return the iterator
     * @throws IOException
     *             Signals that an I/O exception has occurred.
     */
    Iterator<String> resolveChunks(String blobId) throws IOException;
}
