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

package org.apache.jackrabbit.oak.plugins.tika;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static java.util.Arrays.asList;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import com.google.common.collect.Maps;
import com.google.common.io.Closer;
import com.mongodb.MongoClientURI;
import com.mongodb.MongoURI;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.jackrabbit.aws.ext.ds.S3DataStore;
import org.apache.jackrabbit.core.data.DataStore;
import org.apache.jackrabbit.core.data.DataStoreException;
import org.apache.jackrabbit.core.data.FileDataStore;
import org.apache.jackrabbit.oak.commons.PropertiesUtil;
import org.apache.jackrabbit.oak.plugins.blob.datastore.DataStoreBlobStore;
import org.apache.jackrabbit.oak.plugins.blob.datastore.DataStoreTextWriter;
import org.apache.jackrabbit.oak.plugins.document.DocumentMK;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeStore;
import org.apache.jackrabbit.oak.plugins.document.util.MongoConnection;
import org.apache.jackrabbit.oak.plugins.segment.file.FileStore;
import org.apache.jackrabbit.oak.plugins.segment.file.InvalidFileStoreVersionException;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TextExtractorMain {
    private static final Logger log = LoggerFactory.getLogger(TextExtractorMain.class);

    public static void main(String[] args) throws Exception {
        Closer closer = Closer.create();
        String h = "tika [extract|report|generate]\n" +
                "\n" +
                "report   : Generates a summary report related to binary data\n" +
                "extract  : Performs the text extraction\n" +
                "generate : Generates the csv data file based on configured NodeStore/BlobStore";
        try {
            OptionParser parser = new OptionParser();
            OptionSpec<?> help = parser.acceptsAll(asList("h", "?", "help"),
                    "show help").forHelp();

            OptionSpec<String> nodeStoreSpec = parser
                    .accepts("nodestore", "NodeStore detail /path/to/oak/repository | mongodb://host:port/database")
                    .withRequiredArg()
                    .ofType(String.class);

            OptionSpec segmentTar = parser
                    .accepts("segment-tar", "Use oak-segment-tar instead of oak-segment");

            OptionSpec<String> pathSpec = parser
                    .accepts("path", "Path in repository under which the binaries would be searched")
                    .withRequiredArg()
                    .ofType(String.class);

            OptionSpec<File> dataFileSpec = parser
                    .accepts("data-file", "Data file in csv format containing the binary metadata")
                    .withRequiredArg()
                    .ofType(File.class);

            OptionSpec<File> tikaConfigSpec = parser
                    .accepts("tika-config", "Tika config file path")
                    .withRequiredArg()
                    .ofType(File.class);

            OptionSpec<File> fdsDirSpec = parser
                    .accepts("fds-path", "Path of directory used by FileDataStore")
                    .withRequiredArg()
                    .ofType(File.class);

            OptionSpec<File> s3ConfigSpec = parser
                    .accepts("s3-config-path", "Path of properties file containing config for S3DataStore")
                    .withRequiredArg()
                    .ofType(File.class);

            OptionSpec<File> storeDirSpec = parser
                    .accepts("store-path", "Path of directory used to store extracted text content")
                    .withRequiredArg()
                    .ofType(File.class);

            OptionSpec<Integer> poolSize = parser
                    .accepts("pool-size", "Size of the thread pool used to perform text extraction. Defaults " +
                            "to number of cores on the system")
                    .withRequiredArg()
                    .ofType(Integer.class);

            OptionSpec<String> nonOption = parser.nonOptions(h);

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

            boolean report = nonOptions.contains("report");
            boolean extract = nonOptions.contains("extract");
            boolean generate = nonOptions.contains("generate");
            File dataFile = null;
            File storeDir = null;
            File tikaConfigFile = null;
            BlobStore blobStore = null;
            BinaryResourceProvider binaryResourceProvider = null;
            BinaryStats stats = null;
            String path = "/";

            if (options.has(tikaConfigSpec)) {
                tikaConfigFile = tikaConfigSpec.value(options);
                checkArgument(tikaConfigFile.exists(), "Tika config file %s does not exist",
                        tikaConfigFile.getAbsolutePath());
            }

            if (options.has(storeDirSpec)) {
                storeDir = storeDirSpec.value(options);
                if (storeDir.exists()) {
                    checkArgument(storeDir.isDirectory(), "Path [%s] specified for storing extracted " +
                            "text content '%s' is not a directory", storeDir.getAbsolutePath(), storeDirSpec.options());
                }
            }

            if (options.has(fdsDirSpec)) {
                File fdsDir = fdsDirSpec.value(options);
                checkArgument(fdsDir.exists(), "FileDataStore %s does not exist", fdsDir.getAbsolutePath());
                FileDataStore fds = new FileDataStore();
                fds.setPath(fdsDir.getAbsolutePath());
                fds.init(null);
                blobStore = new DataStoreBlobStore(fds);
            }

            if (options.has(s3ConfigSpec)){
                File s3Config = s3ConfigSpec.value(options);
                checkArgument(s3Config.exists() && s3Config.canRead(), "S3DataStore config cannot be read from [%s]",
                        s3Config.getAbsolutePath());
                Properties props = loadProperties(s3Config);
                log.info("Loaded properties for S3DataStore from {}", s3Config.getAbsolutePath());
                String pathProp = "path";
                String repoPath = props.getProperty(pathProp);
                checkNotNull(repoPath, "Missing required property [%s] from S3DataStore config loaded from [%s]", pathProp, s3Config);

                //Check if 'secret' key is defined. It should be non null for references
                //to be generated. As the ref are transient we can just use any random value
                //if not specified
                String secretConfig = "secret";
                if (props.getProperty(secretConfig) == null){
                    props.setProperty(secretConfig, UUID.randomUUID().toString());
                }

                log.info("Using {} for S3DataStore ", repoPath);
                DataStore ds = createS3DataStore(props);
                PropertiesUtil.populate(ds, toMap(props), false);
                ds.init(pathProp);
                blobStore = new DataStoreBlobStore(ds);
                closer.register(asCloseable(ds));
            }

            if (options.has(dataFileSpec)) {
                dataFile = dataFileSpec.value(options);
            }

            checkNotNull(dataFile, "Data file not configured with %s", dataFileSpec);

            if (report || extract) {
                checkArgument(dataFile.exists(),
                        "Data file %s does not exist", dataFile.getAbsolutePath());

                binaryResourceProvider = new CSVFileBinaryResourceProvider(dataFile, blobStore);
                if (binaryResourceProvider instanceof Closeable) {
                    closer.register((Closeable) binaryResourceProvider);
                }

                stats = new BinaryStats(tikaConfigFile, binaryResourceProvider);
                String summary = stats.getSummary();
                log.info(summary);
            }

            if (generate){
                String src = nodeStoreSpec.value(options);
                checkNotNull(blobStore, "BlobStore found to be null. FileDataStore directory " +
                        "must be specified via %s", fdsDirSpec.options());
                checkNotNull(dataFile, "Data file path not provided");
                NodeStore nodeStore = bootStrapNodeStore(src, options.has(segmentTar), blobStore, closer);
                BinaryResourceProvider brp = new NodeStoreBinaryResourceProvider(nodeStore, blobStore);
                CSVFileGenerator generator = new CSVFileGenerator(dataFile);
                generator.generate(brp.getBinaries(path));
            }

            if (extract) {
                checkNotNull(storeDir, "Directory to store extracted text content " +
                        "must be specified via %s", storeDirSpec.options());
                checkNotNull(blobStore, "BlobStore found to be null. FileDataStore directory " +
                        "must be specified via %s", fdsDirSpec.options());

                DataStoreTextWriter writer = new DataStoreTextWriter(storeDir, false);
                TextExtractor extractor = new TextExtractor(writer);

                if (options.has(poolSize)) {
                    extractor.setThreadPoolSize(poolSize.value(options));
                }

                if (tikaConfigFile != null) {
                    extractor.setTikaConfig(tikaConfigFile);
                }

                if (options.has(pathSpec)) {
                    path = pathSpec.value(options);
                }

                closer.register(writer);
                closer.register(extractor);

                extractor.setStats(stats);
                log.info("Using path {}", path);
                extractor.extract(binaryResourceProvider.getBinaries(path));

                extractor.close();
                writer.close();
            }

        } catch (Throwable e) {
            throw closer.rethrow(e);
        } finally {
            closer.close();
        }
    }

    private static Map<String, ?> toMap(Properties properties) {
        Map<String, String> map = Maps.newHashMap();
        for (final String name: properties.stringPropertyNames()) {
            map.put(name, properties.getProperty(name));
        }
        return map;
    }

    private static DataStore createS3DataStore(Properties props) throws IOException {
        S3DataStore s3ds = new S3DataStore();
        s3ds.setProperties(props);
        return s3ds;
    }

    private static Properties loadProperties(File s3Config) throws IOException {
        Properties props = new Properties();
        InputStream is = FileUtils.openInputStream(s3Config);
        try{
            props.load(is);
        } finally {
            IOUtils.closeQuietly(is);
        }
        return props;
    }

    private static NodeStore bootStrapNodeStore(String src, boolean segmentTar, BlobStore blobStore, Closer closer) throws IOException, InvalidFileStoreVersionException {
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
                    .setBlobStore(blobStore)
                    .setMongoDB(mongo.getDB()).getNodeStore();
            closer.register(asCloseable(store));
            return store;
        }

        if (segmentTar) {
            return SegmentTarUtils.bootstrap(src, blobStore, closer);
        }

        return SegmentUtils.bootstrap(src, blobStore, closer);
    }

    private static Closeable asCloseable(final FileStore fs) {
        return new Closeable() {
            @Override
            public void close() throws IOException {
                fs.close();
            }
        };
    }

    private static Closeable asCloseable(final DataStore ds) {
        return new Closeable() {
            @Override
            public void close() throws IOException {
                try {
                    ds.close();
                } catch (DataStoreException e) {
                    throw new IOException(e);
                }
            }
        };
    }

    private static Closeable asCloseable(final DocumentNodeStore dns) {
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
}
