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
import java.util.List;

import org.apache.jackrabbit.oak.plugins.document.DocumentMK;
import org.apache.jackrabbit.oak.plugins.document.util.MongoConnection;
import org.apache.jackrabbit.oak.plugins.segment.SegmentNodeStore;
import org.apache.jackrabbit.oak.plugins.segment.file.FileStore;
import org.apache.jackrabbit.oak.spi.state.NodeStore;

import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;

/**
 * A command line console.
 */
public class Console {

    public static void main(String[] args) throws Exception {
        OptionParser parser = new OptionParser();
        OptionSpec<Integer> clusterId = parser.accepts("clusterId", "MongoMK clusterId")
                .withRequiredArg().ofType(Integer.class).defaultsTo(1);

        OptionSet options = parser.parse(args);
        List<String> nonOptions = options.nonOptionArguments();

        if (nonOptions.isEmpty()) {
            System.err.println("usage: console {<path-to-repository> | <mongodb-uri>}");
            System.exit(1);
        }

        NodeStore store;
        if (nonOptions.get(0).startsWith("mongodb://")) {
            MongoConnection mongo = new MongoConnection(nonOptions.get(0));
            store = new DocumentMK.Builder().
                    setMongoDB(mongo.getDB()).
                    setClusterId(clusterId.value(options)).getNodeStore();
        } else {
            store = new SegmentNodeStore(new FileStore(
                    new File(nonOptions.get(0)), 256));
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
