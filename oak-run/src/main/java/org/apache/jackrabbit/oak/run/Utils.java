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
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Dictionary;
import java.util.Enumeration;
import java.util.List;
import java.util.Properties;

import javax.annotation.Nullable;
import javax.jcr.RepositoryException;

import com.google.common.collect.Maps;
import com.google.common.io.Closer;
import com.mongodb.MongoClientURI;
import com.mongodb.MongoURI;
import joptsimple.ArgumentAcceptingOptionSpec;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import org.apache.felix.cm.file.ConfigurationHandler;
import org.apache.jackrabbit.core.data.DataStore;
import org.apache.jackrabbit.core.data.DataStoreException;
import org.apache.jackrabbit.oak.plugins.blob.datastore.DataStoreBlobStore;
import org.apache.jackrabbit.oak.plugins.blob.datastore.OakFileDataStore;
import org.apache.jackrabbit.oak.blob.cloud.aws.s3.SharedS3DataStore;
import org.apache.jackrabbit.oak.plugins.document.DocumentMK;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeStore;
import org.apache.jackrabbit.oak.plugins.document.util.MongoConnection;
import org.apache.jackrabbit.oak.plugins.segment.file.FileStore;
import org.apache.jackrabbit.oak.plugins.segment.file.InvalidFileStoreVersionException;
import org.apache.jackrabbit.oak.spi.blob.GarbageCollectableBlobStore;
import org.apache.jackrabbit.oak.spi.state.NodeStore;

class Utils {

    public static NodeStore bootstrapNodeStore(String[] args, Closer closer, String h) throws IOException, InvalidFileStoreVersionException {
        //TODO add support for other NodeStore flags
        OptionParser parser = new OptionParser();
        OptionSpec<Integer> clusterId = parser
                .accepts("clusterId", "MongoMK clusterId").withRequiredArg()
                .ofType(Integer.class).defaultsTo(0);
        OptionSpec segmentTar = parser.accepts("segment-tar", "Use oak-segment-tar instead of oak-segment");
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
            DocumentNodeStore store = new DocumentMK.Builder()
                    .setMongoDB(mongo.getDB())
                    .setLeaseCheck(false)
                    .setClusterId(clusterId.value(options)).getNodeStore();
            closer.register(asCloseable(store));
            return store;
        }

        if (options.has(segmentTar)) {
            return SegmentTarUtils.bootstrapNodeStore(src, closer);
        }

        return SegmentUtils.bootstrapNodeStore(src, closer);
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

        OptionSet options = parser.parse(args);

        if (!options.has(s3dsConfig) && !options.has(fdsConfig)) {
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

    static Closeable asCloseable(final FileStore fs) {
        return new Closeable() {

            @Override
            public void close() throws IOException {
                fs.close();
            }
        };
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
