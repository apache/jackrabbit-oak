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
package org.apache.jackrabbit.oak.plugins.blob.cloud;

import org.apache.jackrabbit.oak.commons.PropertiesUtil;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.apache.jackrabbit.oak.plugins.blob.BlobStoreBuilder;
import org.apache.jackrabbit.oak.plugins.blob.BlobStoreConfiguration;

import com.google.common.base.Optional;

/**
 * A factory helper for creating CloudBlobStore instance.
 */
public class CloudBlobStoreBuilder implements BlobStoreBuilder {

    private static final CloudBlobStoreBuilder INSTANCE = new CloudBlobStoreBuilder();

    public static CloudBlobStoreBuilder newInstance() {
        return INSTANCE;
    }

    /**
     * Creates the {@link CloudBlobStore} instance.
     * 
     * @param configuration
     *            the configuration
     * @return the blob store wrapped as {@link Optional} to indicate that the
     *         value might be null when a valid configuration object not
     *         available
     * @throws Exception
     *             the exception
     */
    @Override
    public Optional<BlobStore> build(
            BlobStoreConfiguration configuration)
            throws Exception {
        BlobStore blobStore = null;

        blobStore = new CloudBlobStore();
        PropertiesUtil.populate(blobStore, configuration.getConfigMap(), false);
        ((CloudBlobStore) blobStore).init();

        return Optional.of(blobStore);
    }
}