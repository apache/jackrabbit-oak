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
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.jcr.RepositoryException;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.jackrabbit.core.data.CachingDataStore;
import org.apache.jackrabbit.core.data.DataStoreException;
import org.apache.jackrabbit.oak.blob.cloud.s3.S3DataStore;
import org.apache.jackrabbit.oak.plugins.blob.datastore.DataStoreBlobStore;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.io.Closer;
import com.google.common.io.Files;

public class S3DataStoreFactory implements BlobStoreFactory {

    private static final Logger log = LoggerFactory.getLogger(S3DataStoreFactory.class);

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

        for (Object key : new HashSet<Object>(props.keySet())) {
            String value = props.getProperty((String) key);
            props.put(key, stripValue(value));
        }

        this.directory = directory;
        this.tempHomeDir = Files.createTempDir();
        this.ignoreMissingBlobs = ignoreMissingBlobs;
    }

    @Override
    public BlobStore create(Closer closer) throws IOException {
        S3DataStore delegate = new S3DataStore();
        delegate.setProperties(props);
        delegate.setPath(directory);
        try {
            delegate.init(tempHomeDir.getPath());
        } catch (RepositoryException e) {
            throw new IOException(e);
        }
        closer.register(asCloseable(delegate, tempHomeDir));
        if (ignoreMissingBlobs) {
            return new SafeDataStoreBlobStore(delegate);
        } else {
            return new DataStoreBlobStore(delegate);
        }
    }

    private static Closeable asCloseable(final CachingDataStore store, final File tempHomeDir) {
        return new Closeable() {
            @Override
            public void close() throws IOException {
                try {
                    while (!store.getPendingUploads().isEmpty()) {
                        log.info("Waiting for following uploads to finish: " + store.getPendingUploads());
                        Thread.sleep(1000);
                    }
                    store.close();
                    FileUtils.deleteDirectory(tempHomeDir);
                } catch (DataStoreException e) {
                    throw new IOException(e);
                } catch (InterruptedException e) {
                    throw new IOException(e);
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
