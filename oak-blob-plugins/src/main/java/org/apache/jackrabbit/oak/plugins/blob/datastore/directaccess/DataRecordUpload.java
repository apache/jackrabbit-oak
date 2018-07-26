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
package org.apache.jackrabbit.oak.plugins.blob.datastore.directaccess;

import java.net.URI;
import java.util.Collection;

import org.jetbrains.annotations.NotNull;

public interface DataRecordUpload {
    /**
     * Returns a token that uniquely identifies this upload.  This token must be
     * provided in a subsequent call to {@link
     * DataRecordAccessProvider#completeDataRecordUpload(String)}.
     *
     * @return The unique upload token for this upload.
     */
    @NotNull
    String getUploadToken();

    /**
     * The smallest part size the client can send in a multi-part upload (not
     * counting the final part).  There is no guarantee made that splitting the
     * binary into parts of this size can complete the full upload without
     * exhausting the full supply of uploadPartURIs.  In other words, clients
     * wishing to perform a multi-part upload MUST split the binary into parts
     * of at least this size, in bytes, but clients may need to use larger parts
     * in order to upload the entire binary with the number of URIs provided.
     * <p>
     * Note that some backends have lower-bound limits for the size of a part of
     * a multi-part upload.
     *
     * @return The smallest part size acceptable, for multi-part uploads.
     */
    long getMinPartSize();

    /**
     * The largest part size the client can send in a multi-part upload.  The
     * API guarantees that splitting the file into parts of this size will allow
     * the client to complete the multi-part upload without requiring more URIs
     * that those provided, SO LONG AS the file being uploaded is not larger
     * than the maxSize specified in the original call.
     * <p>
     * A smaller size may also be used so long as it exceeds the value returned
     * by {@link #getMinPartSize()}.  Such smaller values may be more desirable
     * for clients who wish to tune uploads to match network conditions;
     * however, the only guarantee offered by the API is that using parts of
     * the size returned by {@link #getMaxPartSize()} will work without using
     * more URIs than those available in the Collection returned by {@link
     * #getUploadURIs()};
     * <p>
     * If a client calls {@link
     * DataRecordAccessProvider#initiateDataRecordUpload(long, int)} with a
     * value of {@code maxUploadSizeInBytes} that ends up being smaller than the
     * actual size of the binary to be uploaded, it may not be possible to
     * complete the upload with the URIs provided.  The client should initiate
     * the transaction again with the correct size.
     * <p>
     * Note that some backends have upper-bound limits for the size of a part of
     * a multi-part upload.
     *
     * @return The largest part size acceptable, for multi-part uploads.
     */
    long getMaxPartSize();

    /**
     * Returns a Collection of direct-writable upload URIs for uploading a file,
     * or file part in the case of multi-part uploading.  This collection may
     * contain only a single URI in the following cases:
     *  - If the client requested 1 as the value of maxNumberOfURIs in a call to
     *    {@link DataRecordAccessProvider#initiateDataRecordUpload(long, int)},
     *    OR
     *  - If the implementing data store does not support multi-part uploading,
     *    OR
     *  - If the client-specified value for maxUploadSizeInBytes in a call to
     *    {@link DataRecordAccessProvider#initiateDataRecordUpload(long, int)}
     *    is less than or equal to the minimum size of a multi-part upload part
     * If the collection contains only a single URI the client should treat that
     * URI as a direct single-put upload and write the entire binary to the
     * single URI.  Otherwise the client may choose to consume up to the entire
     * collection of URIs provided.
     * <p>
     * Note that ordering matters; URIs should be consumed in sequence and not
     * skipped.
     *
     * @return ordered collection of URIs to be consumed in sequence.
     */
    @NotNull
    Collection<URI> getUploadURIs();
}
