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
package org.apache.jackrabbit.oak.jcr.api.binary;

import java.net.URL;
import java.util.Collection;
import javax.annotation.Nonnull;

/**
 * Describes uploading a binary through HTTP requests in a single or multiple parts (chunks).
 * The high-level overview is described in {@link HttpBinaryProvider}.
 *
 * <p>
 * A caller usually needs to pass the returned information to a remote client that is in
 * possession of the actual binary, who then has to upload the binary using HTTP according
 * to the logic described below.
 *
 * <h2>Client upload logic</h2>
 *
 * A binary could be uploaded in a single request or in multiple requests via multiple parts.
 *
 * <p>
 * URLs returned are self-contained, typically time limited and cryptographically signed.
 * They must be used exactly as they are returned.
 *
 * <p>
 * If {@link #getURLParts()} includes only a single entry or if the actual file size is
 * smaller than {@link #getMinPartSize()}, upload must happen in a single request using the
 * first URL.
 *
 * <p>
 * Otherwise, the remote client can split the binary across all the provided part URLs,
 * which will be based on a good default part size. In case the remote client has knowledge about
 * a different, optimized part size (e.g. depending on the network), it could choose a part size
 * in the range between {@link #getMinPartSize()} and {@link #getMaxPartSize()}.
 *
 * <p>
 * With multiple parts, each part is expected to be of the same size, except for the last
 * one which can be smaller.
 *
 * <p>
 * Below is the detailed algorithm for the remote client.
 *
 * <p>
 * The following variables are used:
 * <ul>
 *     <li><code>fileSize</code>: the actual binary size (must be known at this point)</li>
 *     <li><code>minPartSize</code>: the value from {@link #getMinPartSize()}</li>
 *     <li><code>maxPartSize</code>: the value from {@link #getMaxPartSize()}</li>
 *     <li><code>partURLCount</code>: the number of entries in {@link #getURLParts()}</li>
 *     <li><code>partURLs</code>: the entries in {@link #getURLParts()}</li>
 *     <li><code>partSize</code>: the part size to be used in the upload (to be determined in the algorithm)</li>
 * </ul>
 *
 * Steps:
 * <ol>
 *     <li>if (fileSize divided by maxPartSize) is larger than partURLCount, then the client cannot proceed
 *     and will have to request a new set of URLs with the right fileSize as maxSize</li>
 *     <li>if fileSize is smaller than minPartSize, then take the first provided part URL to upload
 *     the entire binary, with partSize = fileSize</li>
 *     <li>
 *         (optional) if the client has more information to optimize, the partSize
 *         can be chosen, under the condition that all of these are true for the partSize:
 *         <ol>
 *             <li>larger than minPartSize</li>
 *             <li>smaller or equal than maxPartSize (unless it is -1 = unlimited)</li>
 *             <li>larger than fileSize divided by partURLCount</li>
 *         </ol>
 *     </li>
 *     <li>otherwise all part URLs are to be used and the partSize = fileSize divided by partURLCount
 *     (integer division, discard modulo which will be the last part)</li>
 *     <li>upload: segment the binary into partSize, for each segment take the next URL from partURLs
 *     (strictly in order!), proceed with a standard HTTP PUT for each, and for the last part use whatever
 *     segment size is left</li>
 *     <li>if a segment fails during upload, retry (up to a certain time out)</li>
 *     <li>after the upload has finished successfully, notify the application, for example through a
 *     complete request, passing the {@link #getUploadToken() upload token}, and the application will have to
 *     call {@link HttpBinaryProvider#completeHttpUpload(String)} with the token</li>
 * </ol>
 *
 * <h2>JSON view</h2>
 *
 * A JSON representation of this interface as passed back to a remote client might look like this:
 * <pre>
 * {
 *     "uploadToken": "aaaa-bbbb-cccc-dddd-eeee-ffff-gggg-hhhh",
 *     "minPartSize": 10485760,
 *     "maxPartSize": 104857600,
 *     "partURLs": [
 *         "http://server.com/upload/1",
 *         "http://server.com/upload/2",
 *         "http://server.com/upload/3",
 *         "http://server.com/upload/4"
 *     ]
 * }
 * </pre>
 * </p>
 *
 */
public interface HttpBinaryUpload {

    /**
     * Returns a token identifying this upload. This is required to finalize the upload
     * at the end by calling {@link HttpBinaryProvider#completeHttpUpload(String)}.
     *
     * <p>
     * The format of this string is depending on the implementation and opaque to consumers.
     * Implementations must ensure that clients cannot spoof tokens.
     *
     * @return a string token identifying this particular upload
     */
    String getUploadToken();

    /**
     * Return the smallest possible part size in bytes. If a consumer wants to choose a custom
     * part size, it cannot be smaller than this value.
     *
     *
     * @see #getMaxPartSize() getMaxPartSize() for the maximum
     *
     * @return smallest possible part size in bytes
     */
    long getMinPartSize();

    /**
     * Return the largest possible part size in bytes. If a consumer wants to choose a custom
     * part size, it cannot be larger than this value. If this returns -1, the maximum is
     * unlimited.
     *
     * @see #getMinPartSize() getMinPartSize() for the minimum
     *
     * @return largest possible part size in bytes or -1 if unlimited
     */
    long getMaxPartSize();

    /**
     * Returns one or more URLs for uploading the binary in one or more parts.
     *
     * <p>
     * In case of multiple URLs, the order in which they are returned is important and defines
     * the order of the parts. Each part covers a given range of the binary in bytes.
     *
     * <p>
     * The consumer of these URLs must keep the following in mind when issuing requests
     * for these URLs:
     *  - Standard HTTP PUT is required as the request method
     *  - The Content-Length header must be set using the size in bytes of the payload
     *  - The Date header must be set, using this format:  yyyy-MM-dd'T'HH:mm:ssX
     *    For example: 1997-08-29T06:14:00Z
     *  - Content-Type MAY be set if known, for single put uploads only
     *  - Payload must include the bytes for the binary being uploaded, or for the
     *    range of bytes for the corresponding part if using multi-part upload
     *
     * @return one or more URLs for uploading the binary in one or more parts
     */
    @Nonnull
    Collection<URL> getUploadURLs();
}
