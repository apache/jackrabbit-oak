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
package org.apache.jackrabbit.oak.plugins.blob.datastore;

import javax.jcr.RepositoryException;

import org.apache.jackrabbit.core.data.Backend;
import org.apache.jackrabbit.core.data.CachingDataStore;
import org.apache.jackrabbit.core.data.DataStore;
import org.apache.jackrabbit.core.data.FileDataStore;
import org.apache.jackrabbit.core.data.MultiDataStore;
import org.apache.jackrabbit.core.data.db.DbDataStore;
import org.apache.jackrabbit.core.util.db.ConnectionFactory;
import org.apache.jackrabbit.oak.commons.PropertiesUtil;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.apache.jackrabbit.oak.plugins.blob.BlobStoreBuilder;
import org.apache.jackrabbit.oak.plugins.blob.BlobStoreConfiguration;

import com.google.common.base.Optional;

/**
 * Helper class to create {@link DataStoreBlobStore} instance and inject the
 * appropriate Jackrabbit {@link DataStore} instance based on the configuration.
 */
public class DataStoreBlobStoreBuilder implements BlobStoreBuilder {

    private static final DataStoreBlobStoreBuilder INSTANCE = new DataStoreBlobStoreBuilder();

    public static DataStoreBlobStoreBuilder newInstance() {
        return INSTANCE;
    }

    /**
     * Creates the wrapper {@link BlobStore} instance for Jackrabbit
     * {@link DataStore}.
     * 
     * @param configuration
     *            the configuration
     * @return the dS blob store wrapped as{@link Optional} indicating that the
     *         value can be null when a valid configuration is not available
     * @throws Exception
     *             the exception
     */
    @Override
    public Optional<BlobStore> build(BlobStoreConfiguration configuration) throws Exception {
        BlobStore blobStore = null;

        DataStore store = getDataStore(configuration);
        if (store != null) {
            blobStore = new DataStoreBlobStore();
            PropertiesUtil.populate(blobStore, configuration.getConfigMap(), false);
            ((DataStoreBlobStore) blobStore).init(store);
        }
        return Optional.fromNullable(blobStore);
    }

    /**
     * Gets the data store based on the DataStoreProvider.
     * 
     * @param config
     *            the data store config
     * @return the data store
     * @throws RepositoryException
     *             the repository exception
     */
    private DataStore getDataStore(BlobStoreConfiguration config) throws Exception {
        return getDataStore(
                 config.getProperty(BlobStoreConfiguration.PROP_DATA_STORE), config);
    }

    private DataStore getDataStore(String dataStoreType, BlobStoreConfiguration config) throws Exception {
        DataStore dataStore = (DataStore) Class.forName(dataStoreType).newInstance();
        PropertiesUtil.populate(dataStore, config.getConfigMap(), false);

        if (dataStore instanceof DbDataStore) {
            ((DbDataStore) dataStore)
                    .setConnectionFactory(new ConnectionFactory());
        }

        if (dataStore instanceof MultiDataStore) {
            DataStore primary =
                    getDataStore(
                            (String) config.getProperty(BlobStoreConfiguration.PRIMARY_DATA_STORE), config);
            DataStore archive =
                    getDataStore(
                            (String) config.getProperty(BlobStoreConfiguration.ARCHIVE_DATA_STORE), config);
            ((MultiDataStore) dataStore)
                    .setPrimaryDataStore(primary);
            ((MultiDataStore) dataStore)
                    .setArchiveDataStore(archive);
            dataStore.init(null);
        } else if (!(dataStore instanceof FileDataStore)
                && !(dataStore instanceof CachingDataStore)) {
            dataStore.init(null);
            return wrapInCachingDataStore(dataStore, config);
        }
        else {
            dataStore.init(null);
        }

        return dataStore;
    }

    private DataStore wrapInCachingDataStore(final DataStore dataStore, BlobStoreConfiguration config) throws Exception {
        CachingDataStore cachingStore = new CachingDataStore() {
            @Override
            protected Backend createBackend() {
                return new DataStoreWrapperBackend(dataStore);
            }

            @Override
            protected String getMarkerFile() {
                return "db.init.done";
            }
        };

        PropertiesUtil.populate(cachingStore, config.getConfigMap(), false);
        cachingStore.init(null);

        return cachingStore;
    }
}
