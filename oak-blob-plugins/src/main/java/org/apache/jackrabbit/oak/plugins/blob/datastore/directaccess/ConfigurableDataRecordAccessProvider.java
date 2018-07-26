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

public interface ConfigurableDataRecordAccessProvider extends DataRecordAccessProvider {
    /**
     * Specifies the number of seconds before a signed download URI will expire.
     * Setting this to 0 is equivalent to turning off the ability to use
     * direct download.
     *
     * @param expirySeconds Number of seconds before a download URI expires.
     */
    void setDirectDownloadURIExpirySeconds(int expirySeconds);

    /**
     * Specifies the maximum number of read URIs to be cached in an in-memory
     * cache.  Setting this to 0 is equivalent to disabling the cache.
     *
     * @param maxSize Number of read URIs to cache.
     */
    void setDirectDownloadURICacheSize(int maxSize);

    /**
     * Specifies the number of seconds before a signed upload URI will expire.
     * Setting this to 0 is equivalent to turning off the ability to use
     * direct upload.
     *
     * @param expirySeconds Number of seconds before an upload URI expires.
     */
    void setDirectUploadURIExpirySeconds(int expirySeconds);

    /**
     * Enables or disables binary transfer acceleration, if supported by the
     * service provider.
     *
     * @param enabled True to enable binary transfer acceleration (if
     *        supported); False otherwise.
     */
    void setBinaryTransferAccelerationEnabled(boolean enabled);
}
