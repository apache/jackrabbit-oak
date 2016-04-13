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
import static org.apache.jackrabbit.oak.plugins.segment.FileStoreHelper.openFileStore;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

import com.google.common.io.Closer;
import com.mongodb.MongoClientURI;
import com.mongodb.MongoURI;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import org.apache.jackrabbit.oak.plugins.document.DocumentMK;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeStore;
import org.apache.jackrabbit.oak.plugins.document.util.MongoConnection;
import org.apache.jackrabbit.oak.plugins.segment.SegmentNodeStore;
import org.apache.jackrabbit.oak.plugins.segment.file.FileStore;
import org.apache.jackrabbit.oak.spi.state.NodeStore;

class Utils {

    public static NodeStore bootstrapNodeStore(String[] args, Closer closer, String h) throws IOException {
        //TODO add support for other NodeStore flags
        OptionParser parser = new OptionParser();
        OptionSpec<Integer> clusterId = parser
                .accepts("clusterId", "MongoMK clusterId").withRequiredArg()
                .ofType(Integer.class).defaultsTo(0);
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

        FileStore fs = openFileStore(src);
        closer.register(asCloseable(fs));
        return SegmentNodeStore.builder(fs).build();
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
}
