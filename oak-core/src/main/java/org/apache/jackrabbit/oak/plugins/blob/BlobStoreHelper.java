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
package org.apache.jackrabbit.oak.plugins.blob;

import org.apache.jackrabbit.mk.blobs.BlobStore;
import org.apache.jackrabbit.oak.plugins.blob.cloud.CloudBlobStore;
import org.apache.jackrabbit.oak.plugins.blob.cloud.CloudBlobStoreBuilder;
import org.apache.jackrabbit.oak.plugins.blob.datastore.DataStoreBlobStore;
import org.apache.jackrabbit.oak.plugins.blob.datastore.DataStoreBlobStoreBuilder;

import com.google.common.base.Optional;
import com.google.common.base.Strings;

/**
 * A factory helper for creating BlobStore objects.
 */
public class BlobStoreHelper {
    /**
     * Creates the appropriate BlobStoreBuilder instance based on the blobType.
     * 
     * @param blobStoreType
     *            the blob type
     * @return the BlobStoreBuilder wrapped as {@link Optional} to indicate that
     *         the builder returned may be null in the case of a default
     *         BlobStoreType
     * @throws Exception
     *             the exception
     */
    public static Optional<BlobStoreBuilder> createFactory(BlobStoreConfiguration config)
            throws Exception {

        BlobStoreBuilder builder = null;
        if (!Strings.isNullOrEmpty(
                config.getProperty(BlobStoreConfiguration.PROP_BLOB_STORE_PROVIDER))) {
            String blobStoreProvider =
                    config.getProperty(BlobStoreConfiguration.PROP_BLOB_STORE_PROVIDER);
            if (blobStoreProvider.equals(CloudBlobStore.class.getName())) {
                builder = CloudBlobStoreBuilder.newInstance();
            } else if (blobStoreProvider.equals(DataStoreBlobStore.class.getName())) {
                builder = DataStoreBlobStoreBuilder.newInstance();
            }
        }

        return Optional.fromNullable(builder);
    }

    /**
     * Creates the appropriate BlobStore instance based on the blobType and the
     * configuration.
     * 
     * @param blobStoreType
     *            the blob type
     * @param config
     *            the config
     * @return the BlobStoreBuilder wrapped as {@link Optional} to indicate that
     *         the builder returned may be null in the case of a default
     *         BlobStoreType or an invalid config
     * @throws Exception
     *             the exception
     */
    public static Optional<BlobStore> create(BlobStoreConfiguration config)
            throws Exception {
        BlobStore blobStore = null;
        BlobStoreBuilder builder = createFactory(config).orNull();

        if ((builder != null) && (config != null)) {
            blobStore = builder.build(config).orNull();
        }
        return Optional.fromNullable(blobStore);
    }
}
