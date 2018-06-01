/**************************************************************************
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
 *
 *************************************************************************/

package org.apache.jackrabbit.oak.jcr.api.binary;

import java.io.Reader;
import java.net.URL;
import java.util.Map;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Describes the necessary HTTP requests for uploading a binary, usually directly to a binary cloud storage.
 * These URLs are self-contained, time limited and cryptographically signed, and they must be used exactly
 * as they are returned.
 *
 * <p>
 * A binary could be uploaded in a single request using a single, standard HTTP PUT,
 * or in multiple requests via multiple parts (chunks).
 *
 * <p>
 *     There are three possible cases:
 *     <ol>
 *         <li>
 *             client can make choice between a single PUT and multipart upload (typical)
 *             <ul>
 *                 <li>{@link #isSinglePutAvailable()} returns TRUE</li>
 *                 <li>{@link #isMultipartAvailable()} returns TRUE</li>
 *                 <li>{@link #getSinglePutURL()} returns a URL</li>
 *                 <li>{@link #getMultipartUploadRequests()} returns an iterator with multiple entries</li>
 *                 <li>{@link #getMultipartCompleteRequest()} might return something and client must respect it (depends on storage implementation)</li>
 *                 <li>{@link #getMultipartAbortRequest()} might return something and client must respect it (depends on storage implementation)</li>
 *             </ul>
 *         </li>
 *         <li>
 *             only multipart upload available (e.g. large files)
 *             <ul>
 *                 <li>{@link #isSinglePutAvailable()} returns false</li>
 *                 <li>{@link #isMultipartAvailable()} returns TRUE</li>
 *                 <li>{@link #getSinglePutURL()} returns null</li>
 *                 <li>{@link #getMultipartUploadRequests()} returns an iterator with multiple entries</li>
 *                 <li>{@link #getMultipartCompleteRequest()} might return something and client must respect it (depends on storage implementation)</li>
 *                 <li>{@link #getMultipartAbortRequest()} might return something and client must respect it (depends on storage implementation)</li>
 *             </ul>
 *         </li>
 *         <li>
 *             only single PUT available (e.g. very small files)
 *             <ul>
 *                 <li>{@link #isSinglePutAvailable()} returns TRUE</li>
 *                 <li>{@link #isMultipartAvailable()} returns false</li>
 *                 <li>{@link #getSinglePutURL()} returns a URL</li>
 *                 <li>{@link #getMultipartUploadRequests()} returns an empty iterator</li>
 *                 <li>{@link #getMultipartCompleteRequest()} returns null</li>
 *                 <li>{@link #getMultipartAbortRequest()} returns null</li>
 *             </ul>
 *         </li>
 *     </ol>
 * </p>
 *
 * <p>
 *     For illustration purposes, a JSON representation if the information from this interface might look like this:
 *     <pre>
 *     {
 *         "totalSize": 1023,
 *         "singlePutUrl": "http://server.com/upload...",
 *         "multipart": {
 *             "parts": [{
 *                     "size": 500,
 *                     "putUrl": "http://server.com/upload/1..."
 *                 },{
 *                     "size": 500,
 *                     "putUrl": "http://server.com/upload/2..."
 *                 },{
 *                     "size": 23,
 *                     "putUrl": "http://server.com/upload/3..."
 *                 }],
 *             "completeRequest": {
 *                 "method": "POST",
 *                 "url": "http://server.com/upload_finish...",
 *                 "headers": {
 *                         "x-complete-123": "abc"
 *                     },
 *                 "body": "<parts><part>1</part><part>2</part><part>3</part></parts>"
 *             },
 *             "abortRequest": {
 *                 "method": "POST",
 *                 "url": "http://server.com/upload_abort...",
 *                 "headers": {
 *                         "x-complete-123": "abc"
 *                     }
 *             }
 *         }
 *     }
 *     </pre>
 *
 *     The presence of the <code>singlePutUrl</code> field would correspond to {@link #isSinglePutAvailable()}.
 *     The presence of the <code>multipart</code> object would correspond to {@link #isMultipartAvailable()}.
 * </p>
 */
public interface BinaryHttpUpload {

    /**
     * One part or chunk in a series of multipart PUT requests to upload a binary.
     */
    interface Part {

        /**
         * Returns the size of this chunk in bytes.
         *
         * @return size of the chunk in bytes
         */
        long getSize();

        /**
         * Returns the URL for a standard HTTP PUT request to upload this particular chunk.
         *
         * @return a URL for a PUT request
         */
        @Nonnull
        URL getPutURL();
    }

    /**
     * Describes a basic HTTP request.
     */
    interface Request {

        /**
         * Returns the HTTP method, such as "POST" or "PUT".
         *
         * @return the HTTP method
         */
        @Nonnull
        String getMethod();

        /**
         * Returns the URL for this request.
         *
         * @return the URL for this request
         */
        @Nonnull
        URL getURL();

        /**
         * Returns the pre-defined HTTP headers that have to be included in this request. Their order might be important.
         * Client might need to add standard headers dynamically.
         *
         * @return pre-defined HTTP headers as key-value map
         */
        @Nonnull
        Map<String, String> getHeaders();

        /**
         * Returns the body for the request as character stream. This might be some XML or other format as required
         * by the binary storage provider.
         *
         * @return the body for the request as character stream
         */
        @Nonnull
        Reader getBody();
    }

    /**
     * Returns true if the binary can be uploaded through a single PUT request. This will be available
     * in {@link #getSinglePutURL()}.
     *
     * @return true if upload is possible through a single PUT request, false if not
     */
    boolean isSinglePutAvailable();

    /**
     * Returns true if the binary can be uploaded using multiple parts or chunks. The necessary requests
     * will be available in {@link #getMultipartUploadRequests()}, {@link #getMultipartCompleteRequest()} and
     * {@link #getMultipartAbortRequest()}.
     *
     * @return true if upload is possible through multiple part requests, false if not
     */
    boolean isMultipartAvailable();

    /**
     * Returns the URL for a standard HTTP PUT request to upload the entire binary in one go, or
     * {@code null} if that is no possible.
     *
     * @return a URL for a single PUT request or null
     */
    @Nullable
    URL getSinglePutURL();

    /**
     * Return the total size of the binary in bytes as was stated in the initial to-be-uploaded request.
     *
     * @return total size of the binary in bytes
     */
    long getTotalSize();

    /**
     * Returns a list of requests for uploading the binary in parts. The order is from the start to the
     * end of the binary. Each part covers a given range of the binary in bytes. This list is based 
     *
     * <p>
     * If multipart upload is not available ({@link #isMultipartAvailable()} returns false), this will
     * return an empty list.
     *
     * @return
     */
    @Nonnull
    Iterable<Part> getMultipartUploadRequests();

    @Nullable
    Request getMultipartCompleteRequest();

    @Nullable
    // only S3, Azure garbage collects automatically
    Request getMultipartAbortRequest();
}
