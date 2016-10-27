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
package org.apache.jackrabbit.oak.console;

import static java.util.Arrays.asList;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.List;

import javax.sql.DataSource;

import com.mongodb.MongoClientURI;
import com.mongodb.MongoURI;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import org.apache.jackrabbit.core.data.FileDataStore;
import org.apache.jackrabbit.oak.plugins.blob.datastore.DataStoreBlobStore;
import org.apache.jackrabbit.oak.plugins.document.DocumentMK;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeStore;
import org.apache.jackrabbit.oak.plugins.document.rdb.RDBDataSourceFactory;
import org.apache.jackrabbit.oak.plugins.document.util.MongoConnection;
import org.apache.jackrabbit.oak.plugins.segment.SegmentNodeStore;
import org.apache.jackrabbit.oak.plugins.segment.SegmentStore;
import org.apache.jackrabbit.oak.plugins.segment.file.FileStore;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.codehaus.groovy.tools.shell.IO;

/**
 * A command line console.
 */
public class Console {

    public static void main(String[] args) throws Exception {
        OptionParser parser = new OptionParser();
        OptionSpec<Integer> clusterId = parser.accepts("clusterId", "MongoMK clusterId")
                .withRequiredArg().ofType(Integer.class).defaultsTo(0);
        OptionSpec quiet = parser.accepts("quiet", "be less chatty");
        OptionSpec shell = parser.accepts("shell", "run the shell after executing files");
        OptionSpec readWrite = parser.accepts("read-write", "connect to repository in read-write mode");
        OptionSpec<String> fdsPathSpec = parser.accepts("fds-path", "Path to FDS store").withOptionalArg().defaultsTo("");
        OptionSpec segment = parser.accepts("segment", "Use oak-segment instead of oak-segment-tar");
        OptionSpec help = parser.acceptsAll(asList("h", "?", "help"), "show help").forHelp();

        // RDB specific options
        OptionSpec<String> rdbjdbcuser = parser.accepts("rdbjdbcuser", "RDB JDBC user").withOptionalArg().defaultsTo("");
        OptionSpec<String> rdbjdbcpasswd = parser.accepts("rdbjdbcpasswd", "RDB JDBC password").withOptionalArg().defaultsTo("");

        OptionSpec<String> nonOption = parser.nonOptions("console {<path-to-repository> | <mongodb-uri>}");

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

        BlobStore blobStore = null;
        String fdsPath = fdsPathSpec.value(options);
        if (!"".equals(fdsPath)) {
            File fdsDir = new File(fdsPath);
            if (fdsDir.exists()) {
                FileDataStore fds = new FileDataStore();
                fds.setPath(fdsDir.getAbsolutePath());
                fds.init(null);

                blobStore = new DataStoreBlobStore(fds);
            }
        }

        boolean readOnly = !options.has(readWrite);

        NodeStoreFixture fixture;
        if (nonOptions.get(0).startsWith(MongoURI.MONGODB_PREFIX)) {
            MongoClientURI uri = new MongoClientURI(nonOptions.get(0));
            if (uri.getDatabase() == null) {
                System.err.println("Database missing in MongoDB URI: " + uri.getURI());
                System.exit(1);
            }
            MongoConnection mongo = new MongoConnection(uri.getURI());

            DocumentMK.Builder builder = new DocumentMK.Builder()
                    .setBlobStore(blobStore)
                    .setMongoDB(mongo.getDB()).
                    setClusterId(clusterId.value(options));
            if (readOnly) {
                builder.setReadOnlyMode();
            }
            DocumentNodeStore store = builder.getNodeStore();
            fixture = new MongoFixture(store);
        } else if (nonOptions.get(0).startsWith("jdbc")) {
            DataSource ds = RDBDataSourceFactory.forJdbcUrl(nonOptions.get(0), rdbjdbcuser.value(options),
                    rdbjdbcpasswd.value(options));
            DocumentMK.Builder builder = new DocumentMK.Builder()
                    .setBlobStore(blobStore)
                    .setRDBConnection(ds).
                    setClusterId(clusterId.value(options));
            if (readOnly) {
                builder.setReadOnlyMode();
            }
            DocumentNodeStore store = builder.getNodeStore();
            fixture = new MongoFixture(store);
        } else if (options.has(segment)) {
            FileStore.Builder fsBuilder = FileStore.builder(new File(nonOptions.get(0)))
                    .withMaxFileSize(256).withDefaultMemoryMapping();
            if (blobStore != null) {
                fsBuilder.withBlobStore(blobStore);
            }
            FileStore store;
            if (readOnly) {
                store = fsBuilder.buildReadOnly();
            } else {
                store = fsBuilder.build();
            }
            fixture = new SegmentFixture(store);
        } else {
            fixture = SegmentTarFixture.create(new File(nonOptions.get(0)), readOnly, blobStore);
        }

        List<String> scriptArgs = nonOptions.size() > 1 ?
                nonOptions.subList(1, nonOptions.size()) : Collections.<String>emptyList();
        IO io = new IO();

        if(options.has(quiet)){
            io.setVerbosity(IO.Verbosity.QUIET);
        }

        if (readOnly) {
            io.out.println("Repository connected in read-only mode. Use '--read-write' for write operations");
        }

        GroovyConsole console =
                new GroovyConsole(ConsoleSession.create(fixture.getStore()), new IO(), fixture);

        int code = 0;
        if(!scriptArgs.isEmpty()){
            code = console.execute(scriptArgs);
        }

        if(scriptArgs.isEmpty() || options.has(shell)){
            code = console.run();
        }

        System.exit(code);
    }

    private static class MongoFixture implements NodeStoreFixture {
        private final DocumentNodeStore nodeStore;

        private MongoFixture(DocumentNodeStore nodeStore) {
            this.nodeStore = nodeStore;
        }

        @Override
        public NodeStore getStore() {
            return nodeStore;
        }

        @Override
        public void close() throws IOException {
            nodeStore.dispose();
        }
    }

    private static class SegmentFixture implements NodeStoreFixture {
        private final SegmentStore segmentStore;
        private final SegmentNodeStore nodeStore;

        private SegmentFixture(SegmentStore segmentStore) {
            this.segmentStore = segmentStore;
            this.nodeStore = SegmentNodeStore.builder(segmentStore).build();
        }

        @Override
        public NodeStore getStore() {
            return nodeStore;
        }

        @Override
        public void close() throws IOException {
            segmentStore.close();
        }
    }
}
