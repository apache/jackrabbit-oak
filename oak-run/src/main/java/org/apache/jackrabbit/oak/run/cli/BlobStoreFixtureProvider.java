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

package org.apache.jackrabbit.oak.run.cli;

import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Dictionary;
import java.util.Enumeration;
import java.util.Properties;

import javax.annotation.CheckForNull;

import com.google.common.collect.Maps;
import com.google.common.io.Closer;
import com.google.common.io.Files;
import org.apache.commons.io.FileUtils;
import org.apache.felix.cm.file.ConfigurationHandler;
import org.apache.jackrabbit.core.data.DataStore;
import org.apache.jackrabbit.core.data.DataStoreException;
import org.apache.jackrabbit.oak.blob.cloud.aws.s3.SharedS3DataStore;
import org.apache.jackrabbit.oak.blob.cloud.azure.blobstorage.AzureDataStore;
import org.apache.jackrabbit.oak.console.BlobStoreFixture;
import org.apache.jackrabbit.oak.plugins.blob.datastore.DataStoreBlobStore;
import org.apache.jackrabbit.oak.plugins.blob.datastore.OakFileDataStore;
import org.apache.jackrabbit.oak.run.cli.BlobStoreOptions.Type;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;

import static org.apache.jackrabbit.oak.commons.PropertiesUtil.populate;

public class BlobStoreFixtureProvider {

    @CheckForNull
    public static BlobStoreFixture create(Options options) throws Exception{
        BlobStoreOptions bsopts = options.getOptionBean(BlobStoreOptions.class);

        if (bsopts == null){
            return null;
        }

        Type bsType = bsopts.getBlobStoreType();

        if (bsType == Type.NONE){
            return null;
        }
        Closer closer = Closer.create();
        DataStore delegate;
        if (bsType == Type.S3){
            SharedS3DataStore s3ds = new SharedS3DataStore();
            Properties props = loadAndTransformProps(bsopts.getS3ConfigPath());
            s3ds.setProperties(props);
            s3ds.init(null);
            delegate = s3ds;
        } else if(bsType == Type.AZURE){
            AzureDataStore azureds = new AzureDataStore();
            String cfgPath = bsopts.getAzureConfigPath();
            Properties props = loadAndTransformProps(cfgPath);
            azureds.setProperties(props);
            File homeDir =  Files.createTempDir();
            azureds.init(homeDir.getAbsolutePath());
            closer.register(asCloseable(homeDir));
            delegate = azureds;
        } else {
            delegate = new OakFileDataStore();
            String cfgPath = bsopts.getFDSConfigPath();
            Properties props = loadAndTransformProps(cfgPath);
            populate(delegate, Maps.fromProperties(props), true);
            delegate.init(null);
        }
        DataStoreBlobStore blobStore = new DataStoreBlobStore(delegate);
        return new DataStoreFixture(blobStore, closer);
    }

    private static class DataStoreFixture implements BlobStoreFixture {
        private final DataStoreBlobStore blobStore;
        private final Closer closer;

        private DataStoreFixture(DataStoreBlobStore blobStore, Closer closer) {
            this.blobStore = blobStore;
            this.closer = closer;
        }

        @Override
        public BlobStore getBlobStore() {
            return blobStore;
        }

        @Override
        public void close() throws IOException {
            closer.close();
            try {
                blobStore.close();
            } catch (DataStoreException e) {
                throw new IOException(e);
            }
        }
    }

    private static Properties loadAndTransformProps(String cfgPath) throws IOException {
        Dictionary dict = ConfigurationHandler.read(new FileInputStream(cfgPath));
        Properties props = new Properties();
        Enumeration keys = dict.keys();
        while (keys.hasMoreElements()) {
            String key = (String) keys.nextElement();
            props.put(key, dict.get(key));
        }
        return props;
    }

    private static Closeable asCloseable(final File dir) {
        return new Closeable() {
            @Override
            public void close() throws IOException {
                FileUtils.deleteDirectory(dir);
            }
        };
    }
}
