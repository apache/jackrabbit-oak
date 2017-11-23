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

package org.apache.jackrabbit.oak.fixture;

import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Dictionary;
import java.util.Enumeration;
import java.util.Map;

import javax.annotation.CheckForNull;

import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import org.apache.commons.io.FileUtils;
import org.apache.felix.cm.file.ConfigurationHandler;
import org.apache.jackrabbit.core.data.DataStore;
import org.apache.jackrabbit.core.data.FileDataStore;
import org.apache.jackrabbit.oak.commons.PropertiesUtil;
import org.apache.jackrabbit.oak.plugins.blob.BlobStoreStats;
import org.apache.jackrabbit.oak.plugins.blob.datastore.DataStoreBlobStore;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.apache.jackrabbit.oak.spi.blob.FileBlobStore;
import org.apache.jackrabbit.oak.spi.blob.MemoryBlobStore;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.jackrabbit.oak.fixture.DataStoreUtils.cleanup;
import static org.apache.jackrabbit.oak.fixture.DataStoreUtils.configureIfCloudDataStore;

public abstract class BlobStoreFixture implements Closeable{
    private final String name;
    protected final String unique;

    public BlobStoreFixture(String name) {
        this.name = name;
        this.unique = getUniqueName(name);
    }

    public abstract BlobStore setUp();

    public abstract void tearDown();

    public abstract long size();

    public void close(){
        tearDown();
    }

    /**
     * Creates an instance of the BlobStoreFixture based on configuration
     * determined from system properties
     *
     * @param basedir directory to be used in case of file based BlobStore
     * @param fallbackToFDS if true then FileDataStore would be used in absence of
     *                      any explicitly defined BlobStore
     */
    @CheckForNull
    public static BlobStoreFixture create(File basedir, boolean fallbackToFDS,
                                          int fdsCacheInMB,
                                          StatisticsProvider statisticsProvider) {

        if(basedir == null) {
            basedir = FileUtils.getTempDirectory();
        }

        String className = System.getProperty("dataStore");
        if (className != null) {
            return getDataStore(basedir, fdsCacheInMB, statisticsProvider);
        }

        String blobStore = System.getProperty("blobStoreType");
        if ("FDS".equals(blobStore) || (blobStore == null && fallbackToFDS)) {
            return getFileDataStore(basedir, DataStoreBlobStore.DEFAULT_CACHE_SIZE, statisticsProvider);
        } else if ("FBS".equals(blobStore)) {
            return getFileBlobStore(basedir);
        } else if ("MEM".equals(blobStore)) {
            return getMemoryBlobStore();
        }

        return null;
    }

    public static BlobStoreFixture getFileDataStore(final File basedir, final int fdsCacheInMB,
                                                    final StatisticsProvider statisticsProvider) {
        return new BlobStoreFixture("FDS") {
            private File storeDir;
            private FileDataStore fds;

            @Override
            public BlobStore setUp() {
                fds = new FileDataStore();
                fds.setMinRecordLength(4092);
                storeDir = new File(basedir, unique);
                fds.init(storeDir.getAbsolutePath());
                configure(fds);
                DataStoreBlobStore bs = new DataStoreBlobStore(fds, true, fdsCacheInMB);
                bs.setBlobStatsCollector(new BlobStoreStats(statisticsProvider));
                configure(bs);
                return bs;
            }

            @Override
            public void tearDown() {
                fds.close();
                FileUtils.deleteQuietly(storeDir);
            }

            @Override
            public long size() {
                return FileUtils.sizeOfDirectory(storeDir);
            }
        };
    }

    public static BlobStoreFixture getFileBlobStore(final File basedir) {
        return new BlobStoreFixture("FBS") {
            private File storeDir;
            private FileBlobStore fbs;

            @Override
            public BlobStore setUp() {
                storeDir = new File(basedir, unique);
                fbs = new FileBlobStore(storeDir.getAbsolutePath());
                configure(fbs);
                return fbs;
            }

            @Override
            public void tearDown() {
                FileUtils.deleteQuietly(storeDir);
            }

            @Override
            public long size() {
                return FileUtils.sizeOfDirectory(storeDir);
            }
        };
    }

    public static BlobStoreFixture getMemoryBlobStore() {
        return new BlobStoreFixture("MEM") {
            private MemoryBlobStore mbs = new MemoryBlobStore();

            @Override
            public BlobStore setUp() {
                return mbs;
            }

            @Override
            public void tearDown() {

            }

            @Override
            public long size() {
                throw new UnsupportedOperationException("Implementation pending");
            }
        };
    }

    public static BlobStoreFixture getDataStore(final File basedir, final int fdsCacheInMB,
                                                final StatisticsProvider statisticsProvider) {
        return new BlobStoreFixture("DS") {
            private DataStore dataStore;
            private BlobStore blobStore;
            private File storeDir;
            private Map<String, ?> config;

            @Override
            public BlobStore setUp() {
                String className = System.getProperty("dataStore");
                checkNotNull(className, "No system property named 'dataStore' defined");
                try {
                    dataStore = Class.forName(className).asSubclass(DataStore.class).newInstance();
                    config = getConfig();
                    configure(dataStore, config);

                    dataStore = configureIfCloudDataStore(className, dataStore, config, unique.toLowerCase(), statisticsProvider);
                    storeDir = new File(basedir, unique);
                    dataStore.init(storeDir.getAbsolutePath());
                    blobStore = new DataStoreBlobStore(dataStore, true, fdsCacheInMB);
                    configure(blobStore);
                    return blobStore;
                } catch (Exception e) {
                    throw new IllegalStateException("Cannot instantiate DataStore " + className, e);
                }
            }

            @Override
            public void tearDown() {
                if (blobStore instanceof DataStoreBlobStore) {
                    try {
                        ((DataStoreBlobStore) blobStore).close();
                        cleanup(storeDir, config, unique.toLowerCase());
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }
            }

            @Override
            public long size() {
                throw new UnsupportedOperationException("Implementation pending");
            }
        };
    }


    //~------------------------------------------------< utility >

    private static String getUniqueName(String name) {
        return String.format("%s-%d", name, System.currentTimeMillis());
    }

    private static void configure(Object o) {
        PropertiesUtil.populate(o, getConfig(), false);
    }

    private static void configure(Object o, Map<String, ?> config) {
        PropertiesUtil.populate(o, config, false);
    }

    public static Map<String, Object> loadAndTransformProps(String cfgPath) throws IOException {
        Dictionary dict = ConfigurationHandler.read(new FileInputStream(cfgPath));
        Map<String, Object> props = Maps.newHashMap();
        Enumeration keys = dict.keys();
        while (keys.hasMoreElements()) {
            String key = (String) keys.nextElement();
            props.put(key, dict.get(key));
        }
        return props;
    }

    public static Map<String, ?> getConfig() {
        // try loading the props from the config file if configured
        String cfgFile = System.getProperty("ds.config");
        Map<String, Object> result = Maps.newHashMap();
        if (!Strings.isNullOrEmpty(cfgFile)) {
            try {
                result = loadAndTransformProps(cfgFile);
            } catch (IOException e) {
            }
        }

        for (Map.Entry<String, ?> e : Maps.fromProperties(System.getProperties()).entrySet()) {
            String key = e.getKey();
            if (key.startsWith("ds.") || key.startsWith("bs.")) {
                key = key.substring(3); //length of bs.
                result.put(key, e.getValue());
            }
        }
        return result;
    }
}
