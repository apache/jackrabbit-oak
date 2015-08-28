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
import java.util.Properties;

import javax.jcr.RepositoryException;

import org.apache.commons.io.IOUtils;
import org.apache.jackrabbit.core.data.CachingDataStore;
import org.apache.jackrabbit.core.data.DataStoreException;
import org.apache.jackrabbit.oak.blob.cloud.aws.s3.S3DataStore;
import org.apache.jackrabbit.oak.plugins.blob.datastore.DataStoreBlobStore;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.io.Closer;

public class S3DataStoreFactory implements BlobStoreFactory {

    private static final Logger log = LoggerFactory.getLogger(S3DataStoreFactory.class);

    private final Properties props;

    private final String directory;

    public S3DataStoreFactory(String configuration, String directory) throws IOException {
        this.props = new Properties();
        final FileReader reader = new FileReader(new File(configuration));
        try {
            props.load(reader);
        } finally {
            IOUtils.closeQuietly(reader);
        }
        this.directory = directory;
    }

    @Override
    public BlobStore create(Closer closer) throws IOException {
        final S3DataStore delegate = new S3DataStore();
        delegate.setProperties(props);
        delegate.setPath(props.getProperty("path"));
        try {
            delegate.init(directory);
        } catch (RepositoryException e) {
            throw new IOException(e);
        }
        closer.register(asCloseable(delegate));
        return new DataStoreBlobStore(delegate);
    }

    private static Closeable asCloseable(final CachingDataStore store) {
        return new Closeable() {
            @Override
            public void close() throws IOException {
                try {
                    while (!store.getPendingUploads().isEmpty()) {
                        log.info("Waiting for following uploads to finish: " + store.getPendingUploads());
                        Thread.sleep(1000);
                    }
                    store.close();
                } catch (DataStoreException e) {
                    throw new IOException(e);
                } catch (InterruptedException e) {
                    throw new IOException(e);
                }
            }
        };
    }
}
