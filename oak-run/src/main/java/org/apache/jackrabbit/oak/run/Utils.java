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
import static org.apache.jackrabbit.oak.plugins.document.mongo.MongoDocumentNodeStoreBuilder.newMongoDocumentNodeStoreBuilder;
import static org.apache.jackrabbit.oak.plugins.document.rdb.RDBDocumentNodeStoreBuilder.newRDBDocumentNodeStoreBuilder;

import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Dictionary;
import java.util.Enumeration;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import javax.annotation.CheckForNull;
import javax.annotation.Nullable;
import javax.jcr.RepositoryException;
import javax.sql.DataSource;

import joptsimple.OptionSpecBuilder;
import org.apache.commons.io.FileUtils;
import org.apache.felix.cm.file.ConfigurationHandler;
import org.apache.jackrabbit.core.data.DataStore;
import org.apache.jackrabbit.core.data.DataStoreException;
import org.apache.jackrabbit.oak.blob.cloud.azure.blobstorage.AzureDataStore;
import org.apache.jackrabbit.oak.blob.cloud.s3.S3DataStore;
import org.apache.jackrabbit.oak.plugins.blob.datastore.DataStoreBlobStore;
import org.apache.jackrabbit.oak.plugins.blob.datastore.OakFileDataStore;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeStore;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeStoreBuilder;
import org.apache.jackrabbit.oak.plugins.document.rdb.RDBDataSourceFactory;
import org.apache.jackrabbit.oak.plugins.document.util.MongoConnection;
import org.apache.jackrabbit.oak.run.cli.DummyDataStore;
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

    public static class NodeStoreOptions {

        public final OptionParser parser;
        public final OptionSpec<String> rdbjdbcuser;
        public final OptionSpec<String> rdbjdbcpasswd;
        public final OptionSpec<Integer> clusterId;
        public final OptionSpec<Void> disableBranchesSpec;
        public final OptionSpec<Integer> cacheSizeSpec;
        public final OptionSpec<?> help;
        public final OptionSpec<String> nonOption;

        protected OptionSet options;

        public NodeStoreOptions(String usage) {
            parser = new OptionParser();
            rdbjdbcuser = parser
                    .accepts("rdbjdbcuser", "RDB JDBC user")
                    .withOptionalArg().defaultsTo("");
            rdbjdbcpasswd = parser
                    .accepts("rdbjdbcpasswd", "RDB JDBC password")
                    .withOptionalArg().defaultsTo("");
            clusterId = parser
                    .accepts("clusterId", "MongoMK clusterId")
                    .withRequiredArg().ofType(Integer.class).defaultsTo(0);
            disableBranchesSpec = parser.
                    accepts("disableBranches", "disable branches");
            cacheSizeSpec = parser.
                    accepts("cacheSize", "cache size")
                    .withRequiredArg().ofType(Integer.class).defaultsTo(0);
            help = parser.acceptsAll(asList("h", "?", "help"),"show help").forHelp();
            nonOption = parser.nonOptions(usage);
        }

        public NodeStoreOptions parse(String[] args) {
            assert(options == null);
            options = parser.parse(args);
            return this;
        }

        public void printHelpOn(OutputStream sink) throws IOException {
            parser.printHelpOn(sink);
            System.exit(2);
        }

        public String getStoreArg() {
            List<String> nonOptions = nonOption.values(options);
            return nonOptions.size() > 0? nonOptions.get(0) : "";
        }

        public List<String> getOtherArgs() {
            List<String> args = new ArrayList<String>(nonOption.values(options));
            if (args.size() > 0) {
                args.remove(0);
            }
            return args;
        }

        public int getClusterId() {
            return clusterId.value(options);
        }

        public boolean disableBranchesSpec() {
            return options.has(disableBranchesSpec);
        }

        public int getCacheSize() {
            return cacheSizeSpec.value(options);
        }

        public String getRDBJDBCUser() {
            return rdbjdbcuser.value(options);
        }

        public String getRDBJDBCPassword() {
            return rdbjdbcpasswd.value(options);
        }
    }

    public static NodeStore bootstrapNodeStore(String[] args, Closer closer, String h) throws IOException {
        return bootstrapNodeStore(new NodeStoreOptions(h).parse(args), closer);
    }

    public static NodeStore bootstrapNodeStore(NodeStoreOptions options, Closer closer) throws IOException {
        String src = options.getStoreArg();
        if (src == null || src.length() == 0) {
            options.printHelpOn(System.err);
            System.exit(1);
        }

        if (src.startsWith(MongoURI.MONGODB_PREFIX) || src.startsWith("jdbc")) {
            DocumentNodeStoreBuilder<?> builder = createDocumentMKBuilder(options, closer);
            if (builder != null) {
                DocumentNodeStore store = builder.build();
                closer.register(asCloseable(store));
                return store;
            }
        }

        return SegmentTarUtils.bootstrapNodeStore(src, closer);
    }

    @CheckForNull
    static DocumentNodeStoreBuilder<?> createDocumentMKBuilder(NodeStoreOptions options,
                                                               Closer closer)
            throws IOException {
        String src = options.getStoreArg();
        if (src == null || src.length() == 0) {
            options.printHelpOn(System.err);
            System.exit(1);
        }
        DocumentNodeStoreBuilder<?> builder;
        if (src.startsWith(MongoURI.MONGODB_PREFIX)) {
            MongoClientURI uri = new MongoClientURI(src);
            if (uri.getDatabase() == null) {
                System.err.println("Database missing in MongoDB URI: "
                        + uri.getURI());
                System.exit(1);
            }
            MongoConnection mongo = new MongoConnection(uri.getURI());
            closer.register(asCloseable(mongo));
            builder = newMongoDocumentNodeStoreBuilder().setMongoDB(mongo.getDB());
        } else if (src.startsWith("jdbc")) {
            DataSource ds = RDBDataSourceFactory.forJdbcUrl(src,
                    options.getRDBJDBCUser(), options.getRDBJDBCPassword());
            builder = newRDBDocumentNodeStoreBuilder().setRDBConnection(ds);
        } else {
            return null;
        }
        builder.
                setLeaseCheck(false).
                setClusterId(options.getClusterId());
        if (options.disableBranchesSpec()) {
            builder.disableBranches();
        }
        int cacheSize = options.getCacheSize();
        if (cacheSize != 0) {
            builder.memoryCacheSize(cacheSize * MB);
        }
        return builder;
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
        OptionSpecBuilder nods = parser.accepts("nods", "No DataStore ");


        OptionSet options = parser.parse(args);

        if (!options.has(s3dsConfig) && !options.has(fdsConfig) && !options.has(azureBlobDSConfig) && !options.has(nods)) {
            return null;
        }

        DataStore delegate;
        if (options.has(s3dsConfig)) {
            S3DataStore s3ds = new S3DataStore();
            String cfgPath = s3dsConfig.value(options);
            Properties props = loadAndTransformProps(cfgPath);
            s3ds.setProperties(props);
            File homeDir =  Files.createTempDir();
            closer.register(asCloseable(homeDir));
            s3ds.init(homeDir.getAbsolutePath());
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
        } else if (options.has(nods)){
            delegate = new DummyDataStore();
            File homeDir =  Files.createTempDir();
            delegate.init(homeDir.getAbsolutePath());
            closer.register(asCloseable(homeDir));
        }
        else {
            delegate = new OakFileDataStore();
            String cfgPath = fdsConfig.value(options);
            Properties props = loadAndTransformProps(cfgPath);
            populate(delegate, asMap(props), true);
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

    private static Map<String, ?> asMap(Properties props) {
        Map<String, Object> map = Maps.newHashMap();
        for (Object key : props.keySet()) {
            map.put((String)key, props.get(key));
        }
        return map;
    }
}
