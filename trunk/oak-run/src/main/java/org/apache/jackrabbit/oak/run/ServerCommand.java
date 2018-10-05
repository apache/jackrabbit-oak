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

import java.io.File;
import java.net.URI;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.fixture.OakFixture;
import org.apache.jackrabbit.oak.run.commons.Command;

class ServerCommand implements Command {

    private static final String DEFAULT_URI = "http://localhost:8080/";

    private static final int MB = 1024 * 1024;

    private static void startOakServer(OakFixture oakFixture, String uri, List<Integer> cIds) throws Exception {
        Map<Oak, String> m;
        if (cIds.isEmpty()) {
            System.out.println("Starting " + oakFixture.toString() + " repository -> " + uri);
            m = Collections.singletonMap(oakFixture.getOak(0), "");
        } else {
            System.out.println("Starting a clustered repository " + oakFixture.toString() + " -> " + uri);
            m = new HashMap<Oak, String>(cIds.size());

            for (int i = 0; i < cIds.size(); i++) {
                m.put(oakFixture.getOak(i), "/node" + i);
            }
        }
        new HttpServer(URI.create(uri).getPort(), m);
    }

    @Override
    public void execute(String... args) throws Exception {
        OptionParser parser = new OptionParser();

        OptionSpec<Integer> cache = parser.accepts("cache", "cache size (MB)").withRequiredArg().ofType(Integer.class).defaultsTo(100);

        // tar/h2 specific option
        OptionSpec<File> base = parser.accepts("base", "Base directory").withRequiredArg().ofType(File.class);
        OptionSpec<Boolean> mmap = parser.accepts("mmap", "TarMK memory mapping").withOptionalArg().ofType(Boolean.class).defaultsTo("64".equals(System.getProperty("sun.arch.data.model")));

        // mongo specific options:
        OptionSpec<String> host = parser.accepts("host", "MongoDB host").withRequiredArg().defaultsTo("127.0.0.1");
        OptionSpec<Integer> port = parser.accepts("port", "MongoDB port").withRequiredArg().ofType(Integer.class).defaultsTo(27017);
        OptionSpec<String> dbName = parser.accepts("db", "MongoDB database").withRequiredArg();
        OptionSpec<Integer> clusterIds = parser.accepts("clusterIds", "Cluster Ids").withOptionalArg().ofType(Integer.class).withValuesSeparatedBy(',');

        // RDB specific options
        OptionSpec<String> rdbjdbcuri = parser.accepts("rdbjdbcuri", "RDB JDBC URI").withOptionalArg().defaultsTo("");
        OptionSpec<String> rdbjdbcuser = parser.accepts("rdbjdbcuser", "RDB JDBC user").withOptionalArg().defaultsTo("");
        OptionSpec<String> rdbjdbcpasswd = parser.accepts("rdbjdbcpasswd", "RDB JDBC password").withOptionalArg().defaultsTo("");
        OptionSpec<String> rdbjdbctableprefix = parser.accepts("rdbjdbctableprefix", "RDB JDBC table prefix")
                .withOptionalArg().defaultsTo("");

        OptionSpec<String> nonOption = parser.nonOptions();
        OptionSpec<?> help = parser.acceptsAll(asList("h", "?", "help"), "show help").forHelp();
        OptionSet options = parser.parse(args);

        if (options.has(help)) {
            parser.printHelpOn(System.out);
            System.exit(0);
        }

        OakFixture oakFixture;

        List<String> arglist = nonOption.values(options);
        String uri = (arglist.isEmpty()) ? DEFAULT_URI : arglist.get(0);
        String fix = (arglist.size() <= 1) ? OakFixture.OAK_MEMORY : arglist.get(1);

        int cacheSize = cache.value(options);
        List<Integer> cIds = Collections.emptyList();
        if (fix.startsWith(OakFixture.OAK_MEMORY)) {
            if (OakFixture.OAK_MEMORY_NS.equals(fix)) {
                oakFixture = OakFixture.getMemoryNS(cacheSize * MB);
            } else {
                oakFixture = OakFixture.getMemory(cacheSize * MB);
            }
        } else if (fix.startsWith(OakFixture.OAK_MONGO)) {
            cIds = clusterIds.values(options);
            String db = dbName.value(options);
            if (db == null) {
                throw new IllegalArgumentException("Required argument db missing");
            }
            if (OakFixture.OAK_MONGO_NS.equals(fix)) {
                oakFixture = OakFixture.getMongoNS(
                        host.value(options), port.value(options),
                        db, false,
                        cacheSize * MB);
            } else {
                oakFixture = OakFixture.getMongo(
                        host.value(options), port.value(options),
                        db, false, cacheSize * MB);
            }
        } else if (fix.equals(OakFixture.OAK_SEGMENT_TAR)) {
            File baseFile = base.value(options);
            if (baseFile == null) {
                throw new IllegalArgumentException("Required argument base missing.");
            }
            oakFixture = OakFixture.getVanillaSegmentTar(baseFile, 256, cacheSize, mmap.value(options));
        } else if (fix.equals(OakFixture.OAK_RDB)) {
            oakFixture = OakFixture.getRDB(OakFixture.OAK_RDB, rdbjdbcuri.value(options), rdbjdbcuser.value(options),
                    rdbjdbcpasswd.value(options), rdbjdbctableprefix.value(options), false, cacheSize, -1);
        } else {
            throw new IllegalArgumentException("Unsupported repository setup " + fix);
        }

        startOakServer(oakFixture, uri, cIds);
    }

}