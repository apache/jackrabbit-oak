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

package org.apache.jackrabbit.oak.api.blob;

import org.apache.jackrabbit.oak.api.Blob;

import java.net.URL;

public interface HttpBlobProvider {
    /**
     * Begin a transaction to perform a direct binary upload to the cloud storage.
     *
     * @param maxUploadSizeInBytes - the largest size of the binary to be uploaded,
     *                             in bytes, based on the caller's best guess.  If
     *                             the actual size of the file to be uploaded is known,
     *                             that value should be used.
     * @param maxNumberOfURLs - the maximum number of URLs the client is able to accept.
     *                        If the client does not support multi-part uploading, this
     *                        value should be 1.  Note that the implementing class is not
     *                        required to support multi-part uploading so it may return
     *                        only a single upload URL regardless of the value passed in
     *                        for this parameter.
     * @return A {@code BlobHttpUpoad} referencing this direct upload.
     * @throws {@code HttpUploadException} if the upload cannot be completed as
     * requested.
     */
    BlobHttpUpload initiateHttpUpload(long maxUploadSizeInBytes, int maxNumberOfURLs);

    Blob completeHttpUpload(String uploadToken);

    URL getDownloadURL(Blob blob);
}
