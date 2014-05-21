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

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.util.Set;

import org.apache.jackrabbit.oak.fixture.OakFixture;
import org.apache.jackrabbit.oak.plugins.document.DocumentMK;
import org.apache.jackrabbit.oak.plugins.document.util.MongoConnection;
import org.apache.jackrabbit.oak.plugins.segment.SegmentNodeStore;
import org.apache.jackrabbit.oak.plugins.segment.file.FileStore;
import org.apache.jackrabbit.oak.spi.state.NodeStore;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;

/**
 * A command line console.
 */
public class Console {

    public static void main(String[] args) throws Exception {
        OptionParser parser = new OptionParser();
        OptionSpec<File> base = parser.accepts("base", "Base directory")
                .withRequiredArg().ofType(File.class)
                .defaultsTo(new File("."));
        OptionSpec<String> host = parser.accepts("host", "MongoDB host")
                .withRequiredArg().defaultsTo("localhost");
        OptionSpec<Integer> port = parser.accepts("port", "MongoDB port")
                .withRequiredArg().ofType(Integer.class).defaultsTo(27017);
        OptionSpec<String> dbName = parser.accepts("db", "MongoDB database")
                .withRequiredArg().defaultsTo("oak");
        OptionSpec<Integer> clusterId = parser.accepts("clusterId", "MongoMK clusterId")
                .withRequiredArg().ofType(Integer.class).defaultsTo(1);
        OptionSpec<Integer> cache = parser.accepts("cache", "cache size (MB)")
                .withRequiredArg().ofType(Integer.class).defaultsTo(100);

        OptionSet options = parser.parse(args);
        Set<String> argset = Sets.newHashSet(options.nonOptionArguments());

        String uri = "mongodb://" + host.value(options) + ":" +
                port.value(options) + "/" + dbName.value(options);
        NodeStore store;
        if (argset.contains(OakFixture.OAK_MONGO)) {
            MongoConnection mongo = new MongoConnection(uri);
            store = new DocumentMK.Builder().
                    setMongoDB(mongo.getDB()).
                    memoryCacheSize(cache.value(options)).
                    setClusterId(clusterId.value(options)).getNodeStore();
        } else if (argset.contains(OakFixture.OAK_TAR)) {
            store = new SegmentNodeStore(new FileStore(base.value(options), 256));
        } else {
            System.out.println("No repository specified. " +
                    "Was expecting one of: " + Lists.newArrayList(
                    OakFixture.OAK_TAR, OakFixture.OAK_MONGO));
            System.exit(1);
            return;
        }

        System.exit(new Console(store, System.in, System.out).run());
    }

    private final NodeStore store;
    private final InputStream in;
    private final OutputStream out;

    public Console(NodeStore store, InputStream in, OutputStream out) {
        this.store = store;
        this.in = in;
        this.out = out;
    }

    public int run() throws Exception {
        int code = 1;
        ConsoleSession session = ConsoleSession.create(store);
        for (;;) {
            prompt();
            String line = readLine(in);
            if (line == null) {
                break;
            }
            Command c = Command.create(line);
            try {
                c.execute(session, in, out);
            } catch (Exception e) {
                e.printStackTrace(new PrintWriter(out));
            }
            if (c.getName().equals("exit")) {
                code = 0;
                break;
            }

        }
        return code;
    }

    private void prompt() throws IOException {
        out.write("> ".getBytes());
    }

    private static String readLine(InputStream in) throws IOException {
        BufferedReader reader = new BufferedReader(new InputStreamReader(in));
        return reader.readLine();
    }
}
