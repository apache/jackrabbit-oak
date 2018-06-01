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

import javax.annotation.Nullable;
import javax.jcr.AccessDeniedException;
import javax.jcr.Session;

/**
 * Extension interface for {@link Session} that provides the capability to upload binary files
 * directly to underlying blob storage.
 */
public interface BinaryUploadProvider extends Session {

    /**
     * Returns HTTP upload instructions for storing a binary of the given {@code size}
     * as {@code nt:file} at the given {@code path}. Metadata such as {@code jcr:content/jcr:mimeType}
     * will be automatically deducted from the information passed in the HTTP upload.
     * {@code jcr:createdBy} will reflect the user id of this session.
     *
     * <p>
     * If this feature is not available, the method will return {@code null}.
     *
     * <p>
     * The returned instructions will usually be time limited and cannot be shared with other users.
     * They must only be returned to authenticated requests corresponding to this session user
     * or trusted system components (service users).
     *
     * <p>
     * If the current session has not enough permissions to add an {@code nt:file} at the path,
     * an {@link AccessDeniedException} will be thrown.
     *
     * <p>
     * This method does not affect the transient session or persist immediately. The node and binary
     * will only be eventually available after the upload has successfully completed.
     *
     * <p>
     * If the upload fails or never happens before the URLs expire, no change will happen to
     * the repository.
     *
     * <p>
     * As this will persist asynchronously after the upload has finished,
     * without possible error feedback to the caller, this will overwrite any node that might
     * exist at the given path at that point in time.
     * If a node ({@code nt:file} or else) already exists at the path, it will be overwritten
     * with the new binary.
     * If ancestors are missing, intermediary nodes of type {@code nt:folder} will be added.
     * If that is not possible, the handling is up to the implementation.
     *
     * @param path the JCR path to add or overwrite the {@code nt:file}
     * @param size the size of the binary to upload (required)
     * @return HTTP upload instructions or {@code null} if the feature is not available
     */
    @Nullable
    BinaryHttpUpload addFileUsingHttpUpload(String path, long size) throws AccessDeniedException;
    
    Binary completeUpload(String uploadId);
}
