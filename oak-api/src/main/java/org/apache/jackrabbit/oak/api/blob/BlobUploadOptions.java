/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the
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

package org.apache.jackrabbit.oak.api.blob;

import org.osgi.annotation.versioning.ProviderType;

/**
 * Download options to be provided to a call to {@link
 * BlobAccessProvider#initiateBlobUpload(long, int, BlobUploadOptions)}.
 * <p>
 * This object is an internal corollary to {@code
 * org.apache.jackrabbit.api.binary.BinaryUploadOptions}.
 */
@ProviderType
public class BlobUploadOptions {
    private boolean domainOverrideIgnored = false;

    public static final BlobUploadOptions DEFAULT = new BlobUploadOptions();

    private BlobUploadOptions() {
        this(false);
    }

    /**
     * Creates a new upload options instance.
     *
     * @param domainOverrideIgnored true if any configured domain override
     *                              should be ignored when generating URIs;
     *                              false otherwise.
     */
    public BlobUploadOptions(boolean domainOverrideIgnored) {
        this.domainOverrideIgnored = domainOverrideIgnored;
    }

    /**
     * Get the setting to determine if any configured domain override should be
     * ignored when generating URIs.
     *
     * Default behavior is to honor (i.e. not ignore) any configured domain
     * override value (false).
     *
     * @return true to ignore the domain override; false otherwise.
     */
    public boolean isDomainOverrideIgnored() {
        return domainOverrideIgnored;
    }
}
