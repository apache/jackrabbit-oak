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

import java.util.List;
import java.util.concurrent.TimeUnit;

import com.google.common.util.concurrent.AbstractScheduledService;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import org.apache.jackrabbit.oak.plugins.segment.file.FileStore;
import org.apache.jackrabbit.oak.plugins.segment.standby.client.StandbyClient;

class StandbyCommand implements Command {

    @Override
    public void execute(String... args) throws Exception {
        final String defaultHost = "127.0.0.1";
        final int defaultPort = 8023;

        final OptionParser parser = new OptionParser();
        final OptionSpec<String> host = parser.accepts("host", "master host").withRequiredArg().ofType(String.class).defaultsTo(defaultHost);
        final OptionSpec<Integer> port = parser.accepts("port", "master port").withRequiredArg().ofType(Integer.class).defaultsTo(defaultPort);
        final OptionSpec<Integer> interval = parser.accepts("interval", "interval between successive executions").withRequiredArg().ofType(Integer.class);
        final OptionSpec<Boolean> secure = parser.accepts("secure", "use secure connections").withRequiredArg().ofType(Boolean.class);
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

        FileStore store = null;
        StandbyClient failoverClient = null;
        try {
            store = openFileStore(nonOptions.get(0));
            failoverClient = new StandbyClient(
                    options.has(host)? options.valueOf(host) : defaultHost,
                    options.has(port)? options.valueOf(port) : defaultPort,
                    store,
                    options.has(secure) && options.valueOf(secure), 10000);
            if (!options.has(interval)) {
                failoverClient.run();
            } else {
                ScheduledSyncService syncService = new ScheduledSyncService(failoverClient, options.valueOf(interval));
                syncService.startAsync();
                syncService.awaitTerminated();
            }
        } finally {
            if (store != null) {
                store.close();
            }
            if (failoverClient != null) {
                failoverClient.close();
            }
        }
    }

    //TODO react to state changes of FailoverClient (triggered via JMX), once the state model of FailoverClient is complete.
    private static class ScheduledSyncService extends AbstractScheduledService {

        private final StandbyClient failoverClient;
        private final int interval;

        public ScheduledSyncService(StandbyClient failoverClient, int interval) {
            this.failoverClient = failoverClient;
            this.interval = interval;
        }

        @Override
        public void runOneIteration() throws Exception {
            failoverClient.run();
        }

        @Override
        protected Scheduler scheduler() {
            return Scheduler.newFixedDelaySchedule(0, interval, TimeUnit.SECONDS);
        }
    }

}
