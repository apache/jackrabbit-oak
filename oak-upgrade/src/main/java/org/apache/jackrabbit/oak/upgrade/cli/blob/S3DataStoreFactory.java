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
package org.apache.jackrabbit.oak.upgrade.cli.blob;

import java.io.Closeable;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.jcr.RepositoryException;

import com.google.common.collect.Maps;
import org.apache.commons.io.IOUtils;
import org.apache.jackrabbit.core.data.DataStoreException;
import org.apache.jackrabbit.oak.blob.cloud.s3.S3DataStore;
import org.apache.jackrabbit.oak.commons.PropertiesUtil;
import org.apache.jackrabbit.oak.plugins.blob.datastore.DataStoreBlobStore;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;

import com.google.common.io.Closer;
import com.google.common.io.Files;
import org.apache.jackrabbit.oak.stats.DefaultStatisticsProvider;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;

public class S3DataStoreFactory implements BlobStoreFactory {

    private static final Pattern STRIP_VALUE_PATTERN = Pattern.compile("^[TILFDXSCB]?\"(.*)\"\\W*$");

    private final Properties props;

    private final String directory;

    private final File tempHomeDir;

    private final boolean ignoreMissingBlobs;

    public S3DataStoreFactory(String configuration, String directory, boolean ignoreMissingBlobs) throws IOException {
        this.props = new Properties();
        FileReader reader = new FileReader(new File(configuration));
        try {
            props.load(reader);
        } finally {
            IOUtils.closeQuietly(reader);
        }

        this.directory = directory;
        this.tempHomeDir = Files.createTempDir();
        this.ignoreMissingBlobs = ignoreMissingBlobs;
    }

    @Override
    public BlobStore create(Closer closer) throws IOException {
        S3DataStore delegate = createDS(directory, props);
        // Initialize a default stats provider
        StatisticsProvider statsProvider = new DefaultStatisticsProvider(Executors.newSingleThreadScheduledExecutor());
        delegate.setStatisticsProvider(statsProvider);
        // Reduce staging purge interval to 60 seconds
        delegate.setStagingPurgeInterval(60);

        try {
            delegate.init(tempHomeDir.getPath());
        } catch (RepositoryException e) {
            throw new IOException(e);
        }
        closer.register(asCloseable(delegate));
        if (ignoreMissingBlobs) {
            return new SafeDataStoreBlobStore(delegate);
        } else {
            return new DataStoreBlobStore(delegate);
        }
    }

    static S3DataStore createDS(String directory, Properties props) {
        Properties strippedProperties = new Properties();
        Map<String, String> map = Maps.newHashMap();

        for (Object key : new HashSet<>(props.keySet())) {
            String strippedValue = stripValue(props.getProperty((String) key));

            strippedProperties.put(key, strippedValue);
            map.put((String) key, strippedValue);
        }

        S3DataStore ds = new S3DataStore();
        ds.setProperties(strippedProperties);
        ds.setPath(directory);
        PropertiesUtil.populate(ds, map, false);
        return ds;
    }

    private static Closeable asCloseable(final S3DataStore store) {
        return new Closeable() {
            @Override
            public void close() throws IOException {
                try {
                    while (store.getStats().get(1).getElementCount() > 0) {
                        Thread.sleep(100);
                    }
                } catch (InterruptedException e) {
                    throw new IOException(e);
                } finally {
                    try {
                        store.close();
                    } catch (DataStoreException e) {
                        throw new IOException(e);
                    }
                }
            }
        };
    }

    static String stripValue(String value) {
        Matcher matcher = STRIP_VALUE_PATTERN.matcher(value);
        if (matcher.matches()) {
            return matcher.group(1);
        } else {
            return value;
        }
    }

    @Override
    public String toString() {
        return String.format("S3DataStore[%s]", directory);
    }
}
