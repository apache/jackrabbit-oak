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

package org.apache.jackrabbit.oak.run;

import static java.util.Arrays.asList;
import static org.apache.jackrabbit.oak.commons.PropertiesUtil.populate;

import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Dictionary;
import java.util.Enumeration;
import java.util.List;
import java.util.Properties;

import javax.annotation.Nullable;
import javax.jcr.RepositoryException;

import org.apache.commons.io.FileUtils;
import org.apache.felix.cm.file.ConfigurationHandler;
import org.apache.jackrabbit.core.data.DataStore;
import org.apache.jackrabbit.core.data.DataStoreException;
import org.apache.jackrabbit.oak.blob.cloud.azure.blobstorage.AzureDataStore;
import org.apache.jackrabbit.oak.blob.cloud.aws.s3.SharedS3DataStore;
import org.apache.jackrabbit.oak.plugins.blob.datastore.DataStoreBlobStore;
import org.apache.jackrabbit.oak.plugins.blob.datastore.OakFileDataStore;
import org.apache.jackrabbit.oak.plugins.document.DocumentMK;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeStore;
import org.apache.jackrabbit.oak.plugins.document.util.MongoConnection;
import org.apache.jackrabbit.oak.spi.blob.GarbageCollectableBlobStore;
import org.apache.jackrabbit.oak.spi.state.NodeStore;

import com.google.common.collect.Maps;
import com.google.common.io.Closer;
import com.google.common.io.Files;
import com.mongodb.MongoClientURI;
import com.mongodb.MongoURI;

import joptsimple.ArgumentAcceptingOptionSpec;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;

class Utils {
    
    private static final long MB = 1024 * 1024;

    public static NodeStore bootstrapNodeStore(String[] args, Closer closer, String h) throws IOException {
        //TODO add support for other NodeStore flags
        OptionParser parser = new OptionParser();
        OptionSpec<Integer> clusterId = parser
                .accepts("clusterId", "MongoMK clusterId").withRequiredArg()
                .ofType(Integer.class).defaultsTo(0);
        OptionSpec<Void> disableBranchesSpec = parser.
                accepts("disableBranches", "disable branches");    
        OptionSpec<Integer> cacheSizeSpec = parser.
                accepts("cacheSize", "cache size").withRequiredArg().
                ofType(Integer.class).defaultsTo(0);         
        OptionSpec<?> help = parser.acceptsAll(asList("h", "?", "help"),
                "show help").forHelp();
        OptionSpec<String> nonOption = parser
                .nonOptions(h);

        OptionSet options = parser.parse(args);
        List<String> nonOptions = nonOption.values(options);

        if (options.has(help)) {
            parser.printHelpOn(System.out);
            System.exit(0);
        }

        if (nonOptions.isEmpty()) {
            parser.printHelpOn(System.err);
            System.exit(1);
        }

        String src = nonOptions.get(0);
        if (src.startsWith(MongoURI.MONGODB_PREFIX)) {
            MongoClientURI uri = new MongoClientURI(src);
            if (uri.getDatabase() == null) {
                System.err.println("Database missing in MongoDB URI: "
                        + uri.getURI());
                System.exit(1);
            }
            MongoConnection mongo = new MongoConnection(uri.getURI());
            closer.register(asCloseable(mongo));
            DocumentMK.Builder builder = new DocumentMK.Builder();
            builder.
                setMongoDB(mongo.getDB()).
                setLeaseCheck(false).
                setClusterId(clusterId.value(options));
            if (options.has(disableBranchesSpec)) {
                builder.disableBranches();
            }
            int cacheSize = cacheSizeSpec.value(options);
            if (cacheSize != 0) {
                builder.memoryCacheSize(cacheSize * MB);
            }
            DocumentNodeStore store = builder.getNodeStore();
            closer.register(asCloseable(store));
            return store;
        }

        return SegmentTarUtils.bootstrapNodeStore(src, closer);
    }

    @Nullable
    public static GarbageCollectableBlobStore bootstrapDataStore(String[] args, Closer closer)
        throws IOException, RepositoryException {
        OptionParser parser = new OptionParser();
        parser.allowsUnrecognizedOptions();

        ArgumentAcceptingOptionSpec<String> s3dsConfig =
            parser.accepts("s3ds", "S3DataStore config").withRequiredArg().ofType(String.class);
        ArgumentAcceptingOptionSpec<String> fdsConfig =
            parser.accepts("fds", "FileDataStore config").withRequiredArg().ofType(String.class);
        ArgumentAcceptingOptionSpec<String> azureBlobDSConfig =
            parser.accepts("azureblobds", "AzureBlobStorageDataStore config").withRequiredArg().ofType(String.class);


        OptionSet options = parser.parse(args);

        if (!options.has(s3dsConfig) && !options.has(fdsConfig) && !options.has(azureBlobDSConfig)) {
            return null;
        }

        DataStore delegate;
        if (options.has(s3dsConfig)) {
            SharedS3DataStore s3ds = new SharedS3DataStore();
            String cfgPath = s3dsConfig.value(options);
            Properties props = loadAndTransformProps(cfgPath);
            s3ds.setProperties(props);
            s3ds.init(null);
            delegate = s3ds;
        } else if (options.has(azureBlobDSConfig)) {
            AzureDataStore azureds = new AzureDataStore();
            String cfgPath = azureBlobDSConfig.value(options);
            Properties props = loadAndTransformProps(cfgPath);
            azureds.setProperties(props);
            File homeDir =  Files.createTempDir();
            azureds.init(homeDir.getAbsolutePath());
            closer.register(asCloseable(homeDir));
            delegate = azureds;
        } else {
            delegate = new OakFileDataStore();
            String cfgPath = fdsConfig.value(options);
            Properties props = loadAndTransformProps(cfgPath);
            populate(delegate, Maps.fromProperties(props), true);
            delegate.init(null);
        }
        DataStoreBlobStore blobStore = new DataStoreBlobStore(delegate);
        closer.register(Utils.asCloseable(blobStore));

        return blobStore;
    }

    static Closeable asCloseable(final DocumentNodeStore dns) {
        return new Closeable() {

            @Override
            public void close() throws IOException {
                dns.dispose();
            }
        };
    }

    private static Closeable asCloseable(final MongoConnection con) {
        return new Closeable() {

            @Override
            public void close() throws IOException {
                con.close();
            }
        };
    }

    static Closeable asCloseable(final DataStoreBlobStore blobStore) {
        return new Closeable() {

            @Override
            public void close() throws IOException {
                try {
                    blobStore.close();
                } catch (DataStoreException e) {
                    throw new IOException(e);
                }
            }
        };
    }

    static Closeable asCloseable(final File dir) {
        return new Closeable() {

            @Override
            public void close() throws IOException {
                FileUtils.deleteDirectory(dir);
            }
        };
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
}
