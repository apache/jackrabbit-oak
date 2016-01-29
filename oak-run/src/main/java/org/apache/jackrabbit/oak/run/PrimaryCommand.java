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

import java.util.Collections;
import java.util.List;

import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import org.apache.jackrabbit.oak.plugins.segment.file.FileStore;
import org.apache.jackrabbit.oak.plugins.segment.standby.server.StandbyServer;

class PrimaryCommand implements Command {

    @Override
    public void execute(String... args) throws Exception {
        final int defaultPort = 8023;

        final OptionParser parser = new OptionParser();
        final OptionSpec<Integer> port = parser.accepts("port", "port to listen").withRequiredArg().ofType(Integer.class).defaultsTo(defaultPort);
        final OptionSpec<Boolean> secure = parser.accepts("secure", "use secure connections").withRequiredArg().ofType(Boolean.class);
        final OptionSpec<String> admissible = parser.accepts("admissible", "list of admissible slave host names or ip ranges").withRequiredArg().ofType(String.class);
        final OptionSpec<?> help = parser.acceptsAll(asList("h", "?", "help"), "show help").forHelp();
        final OptionSpec<String> nonOption = parser.nonOptions("<path to repository>");

        final OptionSet options = parser.parse(args);
        final List<String> nonOptions = nonOption.values(options);

        if (options.has(help)) {
            parser.printHelpOn(System.out);
            System.exit(0);
        }

        if (nonOptions.isEmpty()) {
            parser.printHelpOn(System.err);
            System.exit(1);
        }

        List<String> admissibleSlaves = options.has(admissible) ? options.valuesOf(admissible) : Collections.EMPTY_LIST;

        FileStore store = null;
        StandbyServer failoverServer = null;
        try {
            store = openFileStore(nonOptions.get(0));
            failoverServer = new StandbyServer(
                    options.has(port)? options.valueOf(port) : defaultPort,
                    store,
                    admissibleSlaves.toArray(new String[admissibleSlaves.size()]),
                    options.has(secure) && options.valueOf(secure));
            failoverServer.startAndWait();
        } finally {
            if (store != null) {
                store.close();
            }
            if (failoverServer != null) {
                failoverServer.close();
            }
        }
    }

}
