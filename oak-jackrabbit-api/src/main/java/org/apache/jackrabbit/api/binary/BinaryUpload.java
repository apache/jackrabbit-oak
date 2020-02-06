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
 *
 */

package org.apache.jackrabbit.api.binary;

import java.net.URI;

import org.apache.jackrabbit.api.JackrabbitValueFactory;
import org.jetbrains.annotations.NotNull;
import org.osgi.annotation.versioning.ProviderType;

/**
 * Describes uploading a binary through HTTP requests in a single or multiple
 * parts. This will be returned by
 * {@link JackrabbitValueFactory#initiateBinaryUpload(long, int)}. A high-level
 * overview of the process can be found in {@link JackrabbitValueFactory}.
 *
 * <p>
 * Note that although the API allows URI schemes other than "http(s)", the
 * upload functionality is currently only defined for HTTP.
 *
 * <p>
 * A caller usually needs to pass the information provided by this interface to
 * a remote client that is in possession of the actual binary, who then has to
 * upload the binary using HTTP according to the logic described below. A remote
 * client is expected to support multi-part uploads as per the logic described
 * below, in case multiple URIs are returned.
 *
 * <p>
 * Once a remote client finishes uploading the binary data, the application must
 * be notified and must then call
 * {@link JackrabbitValueFactory#completeBinaryUpload(String)} to complete the
 * upload. This completion requires the exact upload token obtained from
 * {@link #getUploadToken()}.
 *
 * <h2 id="upload.algorithm">Upload algorithm</h2>
 *
 * A remote client will have to follow this algorithm to upload a binary based
 * on the information provided by this interface.
 *
 * <p>
 * Please be aware that if the size passed to
 * {@link JackrabbitValueFactory#initiateBinaryUpload(long, int)} was an
 * estimation, but the actual binary is larger, there is no guarantee the
 * upload will be possible using all {@link #getUploadURIs()} and the
 * {@link #getMaxPartSize()}. In such cases, the application should restart the
 * transaction using the correct size.
 *
 * <h3>Variables used</h3>
 * <ul>
 *     <li>{@code fileSize}: the actual binary size (must be known at this
 *     point)</li>
 *     <li>{@code minPartSize}: the value from {@link #getMinPartSize()}</li>
 *     <li>{@code maxPartSize}: the value from {@link #getMaxPartSize()}</li>
 *     <li>{@code numUploadURIs}: the number of entries in {@link
 *     #getUploadURIs()}</li>
 *     <li>{@code uploadURIs}: the entries in {@link #getUploadURIs()}</li>
 *     <li>{@code partSize}: the part size to be used in the upload (to be
 *     determined in the algorithm)</li>
 * </ul>
 *
 * <h3>Steps</h3>
 * <ol>
 *     <li>
 *         If {@code (fileSize / maxPartSize) > numUploadURIs}, then the client
 *         cannot proceed and will have to request a new set of URIs with the
 *         right fileSize as {@code maxSize}.
 *     </li>
 *     <li>
 *         Calculate the {@code partSize} and the number of URIs to use.
 *         <br>
 *         The easiest way to do this is to use the {@code maxPartSize} as the
 *         value for {@code partSize}.  As long as the size of the actual binary
 *         upload is less than or equal to the size passed to
 *         {@link JackrabbitValueFactory#initiateBinaryUpload(long, int)}, a
 *         non-null BinaryUpload object returned from that call means you are
 *         guaranteed to be able to upload the binary successfully, using the
 *         provided {@code uploadURIs}, so long as the value you use for
 *         {@code partSize} is {@code maxPartSize}.
 *         Note that it is not required to use of all the URIs provided in
 *         {@code uploadURIs} if not all URIs are required to upload the entire
 *         binary with the selected {@code partSize}.
 *         <br>
 *         However, there are some exceptions to consider:
 *         <ol>
 *             <li>
 *                 If {@code fileSize < minPartSize}, then take the first
 *                 provided upload URI to upload the entire binary, with
 *                 {@code partSize = fileSize}.  Note that it is not required to
 *                 use all of the URIs provided in {@code uploadURIs}.
 *             </li>
 *             <li>
 *                 If {@code fileSize / partSize == numUploadURIs}, all part
 *                 URIs must to be used. The {@code partSize} to use for all
 *                 parts except the last would be calculated using:
 *                 <pre>partSize = (fileSize + numUploadURIs - 1) / numUploadURIs</pre>
 *                 It is also possible to simply use {@code maxPartSize} as the
 *                 value for {@code partSize} in this case, for every part
 *                 except the last.
 *             </li>
 *         </ol>
 *         Optionally, a client may select a different {@code partSize},
 *         for example if the client has more information about the
 *         conditions of the network or other information that would
 *         make a different {@code partSize} preferable.  In this case a
 *         different value may be chosen, under the condition that all
 *         of the following are true:
 *         <ol>
 *             <li>{@code partSize >= minPartSize}</li>
 *             <li>{@code partSize <= maxPartSize}
 *             (unless {@code maxPartSize = -1} meaning unlimited)</li>
 *             <li>{@code partSize > (fileSize / numUploadURIs)}</li>
 *         </ol>
 *     </li>
 *     <li>
 *         Upload: segment the binary into {@code partSize}, for each segment
 *         take the next URI from {@code uploadURIs} (strictly in order),
 *         proceed with a standard HTTP PUT for each, and for the last part use
 *         whatever segment size is left.
 *     </li>
 *     <li>
 *         If a segment fails during upload, retry (up to a certain timeout).
 *     </li>
 *     <li>
 *         After the upload has finished successfully, notify the application,
 *         for example through a complete request, passing the {@link
 *         #getUploadToken() upload token}, and the application will call {@link
 *         JackrabbitValueFactory#completeBinaryUpload(String)} with the token.
 *         <br>
 *         The only timeout restrictions for calling
 *         {@link JackrabbitValueFactory#completeBinaryUpload(String)} are those
 *         imposed by the cloud blob storage service on uploaded blocks.  Upload
 *         tokens themselves do not time out, which allows you to be very
 *         lenient in allowing uploads to complete, and very resilient in
 *         handling temporary network issues or other issues that might impact
 *         the uploading of one or more blocks.
 *         <br>
 *         In the case that the upload cannot be finished (for example, one or
 *         more segments cannot be uploaded even after a reasonable number of
 *         retries), do not call
 *         {@link JackrabbitValueFactory#completeBinaryUpload(String)}.
 *         Instead, simply restart the upload from the beginning by calling
 *         {@link JackrabbitValueFactory#initiateBinaryUpload(long, int)} when
 *         the situation preventing a successful upload has been resolved.
 *     </li>
 * </ol>
 *
 * <h2>Example JSON view</h2>
 *
 * A JSON representation of this interface as passed back to a remote client
 * might look like this:
 * 
 * <pre>
 * {
 *     "uploadToken": "aaaa-bbbb-cccc-dddd-eeee-ffff-gggg-hhhh",
 *     "minPartSize": 10485760,
 *     "maxPartSize": 104857600,
 *     "uploadURIs": [
 *         "http://server.com/upload/1",
 *         "http://server.com/upload/2",
 *         "http://server.com/upload/3",
 *         "http://server.com/upload/4"
 *     ]
 * }
 * </pre>
 */
@ProviderType
public interface BinaryUpload {
    /**
     * Returns a list of URIs that can be used for uploading binary data
     * directly to a storage location in one or more parts.
     *
     * <p>
     * Remote clients must support multi-part uploading as per the
     * <a href="#upload.algorithm">upload algorithm</a> described above. Clients
     * are not necessarily required to use all of the URIs provided. A client
     * may choose to use fewer, or even only one of the URIs. However, it must
     * always ensure the part size is between {@link #getMinPartSize()} and
     * {@link #getMaxPartSize()}. These can reflect strict limitations of the
     * storage provider.
     *
     * <p>
     * Regardless of the number of URIs used, they must be consumed in sequence,
     * without skipping any, and the order of parts the original binary is split
     * into must correspond exactly with the order of URIs.
     *
     * <p>
     * For example, if a client wishes to upload a binary in three parts and
     * there are five URIs returned, the client must use the first URI to
     * upload the first part, the second URI to upload the second part, and
     * the third URI to upload the third part. The client is not required to
     * use the fourth and fifth URIs. However, using the second URI to upload
     * the third part may result in either an upload failure or a corrupted
     * upload; likewise, skipping the second URI to use subsequent URIs may
     * result in either an upload failure or a corrupted upload.
     *
     * <p>
     * While the API supports multi-part uploading via multiple upload URIs,
     * implementations are not required to support multi-part uploading. If the
     * underlying implementation does not support multi-part uploading, a single
     * URI will be returned regardless of the size of the data being uploaded.
     *
     * <p>
     * <b>Security considerations:</b>
     *
     * <ul>
     *     <li>
     *         The URIs cannot be shared with other users. They must only be returned to
     *         authenticated requests corresponding to this session user or trusted system
     *         components.
     *     </li>
     *     <li>
     *         The URIs must not be persisted for later use and will typically be time limited.
     *     </li>
     *     <li>
     *         The URIs will only grant access to this particular binary.
     *     </li>
     *     <li>
     *         The client cannot infer any semantics from the URI structure and path names.
     *         It would typically include a cryptographic signature. Any change to the URIs will
     *         likely result in a failing request.
     *     </li>
     * </ul>
     *
     * @return Iterable of URIs that can be used for uploading directly to a
     *         storage location.
     */
    @NotNull
    Iterable<URI> getUploadURIs();

    /**
     * Return the smallest possible part size in bytes. If a consumer wants to
     * choose a custom part size, it cannot be smaller than this value. This
     * does not apply to the final part. This value will be equal or larger than
     * zero.
     *
     * <p>
     * Note that the API offers no guarantees that using this minimal part size
     * is possible with the number of available {@link #getUploadURIs()}. This
     * might not be the case if the binary is too large. Please refer to the
     * <a href="#upload.algorithm">upload algorithm</a> for the correct use of
     * this value.
     *
     * @return The smallest part size acceptable for multi-part uploads.
     */
    long getMinPartSize();

    /**
     * Return the largest possible part size in bytes. If a consumer wants to
     * choose a custom part size, it cannot be larger than this value.
     * If this returns -1, the maximum is unlimited.
     *
     * <p>
     * The API guarantees that a client can split the binary of the requested
     * size using this maximum part size and there will be sufficient URIs
     * available in {@link #getUploadURIs()}. Please refer to the
     * <a href="#upload.algorithm">upload algorithm</a> for the correct use of
     * this value.
     *
     * @return The maximum part size acceptable for multi-part uploads or -1
     *         if there is no limit.
     */
    long getMaxPartSize();

    /**
     * Returns a token identifying this upload. This is required to finalize the upload
     * at the end by calling {@link JackrabbitValueFactory#completeBinaryUpload(String)}.
     *
     * <p>
     * The format of this string is implementation-dependent. Implementations must ensure
     * that clients cannot guess tokens for existing binaries.
     *
     * @return A unique token identifying this upload.
     */
    @NotNull
    String getUploadToken();
}
