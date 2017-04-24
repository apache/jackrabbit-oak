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

import com.google.common.base.Joiner;
import com.google.common.io.Closer;

import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import joptsimple.OptionSpec;

import org.apache.jackrabbit.oak.commons.TimeDurationFormatter;
import org.apache.jackrabbit.oak.plugins.document.VersionGarbageCollector.VersionGCInfo;
import org.apache.jackrabbit.oak.plugins.document.VersionGarbageCollector.VersionGCStats;
import org.apache.jackrabbit.oak.run.commons.Command;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeStore;
import org.apache.jackrabbit.oak.plugins.document.VersionGCOptions;
import org.apache.jackrabbit.oak.plugins.document.VersionGarbageCollector;
import org.apache.jackrabbit.oak.spi.state.NodeStore;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.jackrabbit.oak.plugins.document.util.Utils.timestampToString;

/**
 * Gives information about current node revisions state.
 */
public class RevisionsCommand implements Command {

    private static final String USAGE = Joiner.on(System.lineSeparator()).join(
            "revisions mongodb://host:port/database <sub-command> [options]",
            "where sub-command is one of",
            "  info     give information about the revisions state without performing",
            "           any modifications",
            "  collect  perform garbage collection",
            "  reset    clear all persisted metadata"
    );

    private static class RevisionsOptions extends Utils.NodeStoreOptions {

        static final String CMD_INFO = "info";
        static final String CMD_COLLECT = "collect";
        static final String CMD_RESET = "reset";

        final OptionSpec<?> once;
        final OptionSpec<Integer> limit;
        final OptionSpec<Long> timeLimit;
        final OptionSpec<Long> olderThan;
        final OptionSpec<Double> delay;

        RevisionsOptions(String usage) {
            super(usage);
            once = parser.accepts("once", "only 1 iteration");
            limit = parser
                    .accepts("limit", "collect at most limit documents").withRequiredArg()
                    .ofType(Integer.class).defaultsTo(-1);
            olderThan = parser
                    .accepts("olderThan", "collect only docs older than n seconds").withRequiredArg()
                    .ofType(Long.class).defaultsTo(TimeUnit.DAYS.toSeconds(1));
            delay = parser
                    .accepts("delay", "introduce delays to reduce impact on system").withRequiredArg()
                    .ofType(Double.class).defaultsTo(0.0);
            timeLimit = parser
                    .accepts("timeLimit", "cancel garbage collection after n seconds").withRequiredArg()
                    .ofType(Long.class).defaultsTo(-1L);
        }

        public RevisionsOptions parse(String[] args) {
            super.parse(args);
            return this;
        }

        String getSubCmd() {
            List<String> args = getOtherArgs();
            if (args.size() > 0) {
                return args.get(0);
            }
            return "info";
        }

        boolean runOnce() {
            return options.has(once);
        }

        int getLimit() {
            return limit.value(options);
        }

        long getOlderThan() {
            return olderThan.value(options);
        }

        double getDelay() {
            return delay.value(options);
        }

        long getTimeLimit() {
            return timeLimit.value(options);
        }
    }

    @Override
    public void execute(String... args) throws Exception {
        Closer closer = Closer.create();
        try {
            RevisionsOptions options = new RevisionsOptions(USAGE).parse(args);
            VersionGarbageCollector gc = bootstrapVGC(options, closer);

            String subCmd = options.getSubCmd();
            if (RevisionsOptions.CMD_INFO.equals(subCmd)) {
                info(gc, options.getOlderThan());
            } else if (RevisionsOptions.CMD_COLLECT.equals(subCmd)) {
                collect(gc, options.getOlderThan(), options.getTimeLimit());
            } else if (RevisionsOptions.CMD_RESET.equals(subCmd)) {
                reset(gc);
            } else {
                System.err.println("unknown revisions command: " + subCmd);
            }
        } catch (Throwable e) {
            throw closer.rethrow(e);
        } finally {
            closer.close();
        }
    }

    private VersionGarbageCollector bootstrapVGC(RevisionsOptions options,
                                                 Closer closer)
            throws IOException {
        NodeStore store = Utils.bootstrapNodeStore(options, closer);
        if (!(store instanceof DocumentNodeStore)) {
            System.err.println("revisions mode only available for DocumentNodeStore");
            System.exit(1);
        }
        DocumentNodeStore dns = (DocumentNodeStore) store;
        VersionGarbageCollector gc = dns.getVersionGarbageCollector();

        VersionGCOptions gcOptions = gc.getOptions();
        gcOptions = gcOptions.withDelayFactor(options.getDelay());
        if (options.runOnce()) {
            gcOptions = gcOptions.withMaxIterations(1);
        }
        if (options.getLimit() >= 0) {
            gcOptions = gcOptions.withCollectLimit(options.getLimit());
        }
        gc.setOptions(gcOptions);
        return gc;
    }

    private void info(VersionGarbageCollector gc, long olderThanSec)
            throws IOException {
        System.out.println("retrieving gc info");
        VersionGCInfo info = gc.getInfo(olderThanSec, SECONDS);

        System.out.printf(Locale.US, "%21s  %s%n", "Last Successful Run:",
                info.lastSuccess > 0? fmtTimestamp(info.lastSuccess) : "<unknown>");
        System.out.printf(Locale.US, "%21s  %s%n", "Oldest Revision:",
                fmtTimestamp(info.oldestRevisionEstimate));
        System.out.printf(Locale.US, "%21s  %d%n", "Delete Candidates:",
                info.revisionsCandidateCount);
        System.out.printf(Locale.US, "%21s  %d%n", "Collect Limit:",
                info.collectLimit);
        System.out.printf(Locale.US, "%21s  %s%n", "Collect Interval:",
                fmtDuration(info.recommendedCleanupInterval));
        System.out.printf(Locale.US, "%21s  %s%n", "Collect Before:",
                fmtTimestamp(info.recommendedCleanupTimestamp));
        System.out.printf(Locale.US, "%21s  %d%n", "Iterations Estimate:",
                info.estimatedIterations);
    }

    private void collect(final VersionGarbageCollector gc,
                         final long olderThanSec,
                         final long timeLimit)
            throws IOException {
        long started = System.currentTimeMillis();
        System.out.println("starting gc collect");
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Future<VersionGCStats> f = executor.submit(new Callable<VersionGCStats>() {
                @Override
                public VersionGCStats call() throws Exception {
                    return gc.gc(olderThanSec, SECONDS);
                }
            });
            if (timeLimit >= 0) {
                try {
                    f.get(timeLimit, SECONDS);
                } catch (TimeoutException e) {
                    // cancel the gc
                    gc.cancel();
                } catch (ExecutionException e) {
                    // re-throw any other exception
                    throw new IOException(e.getCause());
                } catch (Exception e) {
                    throw new IOException(e);
                }
            }
            try {
                VersionGCStats stats = f.get();
                long ended = System.currentTimeMillis();
                System.out.printf(Locale.US, "%21s  %s%n", "Started:", fmtTimestamp(started));
                System.out.printf(Locale.US, "%21s  %s%n", "Ended:", fmtTimestamp(ended));
                System.out.printf(Locale.US, "%21s  %s%n", "Duration:", fmtDuration(ended - started));
                System.out.printf(Locale.US, "%21s  %s%n", "Stats:", stats.toString());
            } catch (InterruptedException e) {
                throw new IOException(e);
            } catch (ExecutionException e) {
                throw new IOException(e.getCause());
            }
        } finally {
            executor.shutdownNow();
        }
    }

    private void reset(VersionGarbageCollector gc) {
        System.out.println("resetting recommendations and statistics");
        gc.reset();
    }

    private String fmtTimestamp(long ts) {
        return timestampToString(ts);
    }

    private String fmtDuration(long ts) {
        return TimeDurationFormatter.forLogging().format(ts, TimeUnit.MILLISECONDS);
    }
}
