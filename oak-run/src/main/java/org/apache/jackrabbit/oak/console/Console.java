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
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FilterInputStream;
import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.io.PrintStream;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.jackrabbit.oak.plugins.document.DocumentMK;
import org.apache.jackrabbit.oak.plugins.document.util.MongoConnection;
import org.apache.jackrabbit.oak.plugins.segment.SegmentNodeStore;
import org.apache.jackrabbit.oak.plugins.segment.file.FileStore;
import org.apache.jackrabbit.oak.spi.state.NodeStore;

import com.google.common.collect.Lists;
import com.mongodb.MongoClientURI;
import com.mongodb.MongoURI;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;

import static java.util.Arrays.asList;

/**
 * A command line console.
 */
public class Console {

    public static void main(String[] args) throws Exception {
        OptionParser parser = new OptionParser();
        OptionSpec<Integer> clusterId = parser.accepts("clusterId", "MongoMK clusterId")
                .withRequiredArg().ofType(Integer.class).defaultsTo(1);
        OptionSpec<String> eval = parser.accepts("eval", "Evaluate script")
                .withRequiredArg().ofType(String.class);
        OptionSpec help = parser.acceptsAll(asList("h", "?", "help"), "show help").forHelp();
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

        NodeStore store;
        if (nonOptions.get(0).startsWith(MongoURI.MONGODB_PREFIX)) {
            MongoClientURI uri = new MongoClientURI(nonOptions.get(0));
            if (uri.getDatabase() == null) {
                System.err.println("Database missing in MongoDB URI: " + uri.getURI());
                System.exit(1);
            }
            MongoConnection mongo = new MongoConnection(uri.getURI());
            store = new DocumentMK.Builder().
                    setMongoDB(mongo.getDB()).
                    setClusterId(clusterId.value(options)).getNodeStore();
        } else {
            store = new SegmentNodeStore(new FileStore(
                    new File(nonOptions.get(0)), 256));
        }

        Console console = new Console(store, System.in, System.out);
        String script = eval.value(options);
        if (script != null) {
            Command evalCommand = new Command.Eval();
            evalCommand.setArgs(script);
            System.exit(console.run(evalCommand));
        }

        System.exit(console.run());
    }

    private final InputStream in;
    private final OutputStream out;
    private final ExecutorService executor = Executors.newCachedThreadPool();
    private final ConsoleSession session;

    public Console(NodeStore store, InputStream in, OutputStream out) {
        this.in = new FilterInputStream(in) {
            @Override
            public void close() throws IOException {
                // do not close
            }
        };
        this.out = new FilterOutputStream(out) {
            @Override
            public void close() throws IOException {
                // flush but do not close
                flush();
            }
        };
        this.session = ConsoleSession.create(store);
    }

    public int run() throws Exception {
        int code = 1;
        for (;;) {
            prompt();
            String line = readLine(in);
            if (line == null) {
                break;
            }
            boolean exit = false;
            PipedInputStream input;
            OutputStream output = out;
            List<Command> commands = Command.create(line);
            Collections.reverse(commands);
            for (Iterator<Command> it = commands.iterator(); it.hasNext(); ) {
                Command c = it.next();
                if (c.getName().equals("exit")) {
                    exit = true;
                }
                if (it.hasNext()) {
                    input = new PipedInputStream();
                    c.init(session, input, output);
                    output = new PipedOutputStream(input);
                } else {
                    // first command -> pass an empty stream for now
                    // FIXME: find a way to read from stdin without blocking
                    c.init(session, new ByteArrayInputStream(new byte[0]), output);
                }
            }

            List<Callable<Object>> tasks = Lists.newArrayList();
            for (Command c : commands) {
                tasks.add(c);
            }
            for (Future<Object> result : executor.invokeAll(tasks)) {
                try {
                    result.get();
                } catch (ExecutionException e) {
                    e.getCause().printStackTrace(new PrintStream(out, true));
                }
            }

            if (exit) {
                code = 0;
                break;
            }
        }
        return code;
    }

    public int run(Command c) {
        c.init(session, in, out);
        try {
            c.call();
            return 0;
        } catch (Exception e) {
            e.printStackTrace(new PrintStream(out, true));
            return 1;
        }
    }

    private void prompt() throws IOException {
        out.write("> ".getBytes());
        out.flush();
    }

    private static String readLine(InputStream in) throws IOException {
        BufferedReader reader = new BufferedReader(new InputStreamReader(in));
        return reader.readLine();
    }
}
