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
import com.google.common.collect.ImmutableList;
import com.google.common.io.Closer;

import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.LoggerContext;
import joptsimple.OptionSpec;

import org.apache.jackrabbit.oak.commons.TimeDurationFormatter;
import org.apache.jackrabbit.oak.plugins.document.ClusterNodeInfoDocument;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeStore;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeStoreBuilder;
import org.apache.jackrabbit.oak.plugins.document.DocumentStore;
import org.apache.jackrabbit.oak.plugins.document.FormatVersion;
import org.apache.jackrabbit.oak.plugins.document.MissingLastRevSeeker;
import org.apache.jackrabbit.oak.plugins.document.RevisionContextWrapper;
import org.apache.jackrabbit.oak.plugins.document.SweepHelper;
import org.apache.jackrabbit.oak.plugins.document.VersionGCSupport;
import org.apache.jackrabbit.oak.plugins.document.VersionGarbageCollector.VersionGCInfo;
import org.apache.jackrabbit.oak.plugins.document.VersionGarbageCollector.VersionGCStats;
import org.apache.jackrabbit.oak.run.commons.Command;
import org.apache.jackrabbit.oak.plugins.document.VersionGCOptions;
import org.apache.jackrabbit.oak.plugins.document.VersionGarbageCollector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.jackrabbit.oak.plugins.document.DocumentNodeStoreHelper.createVersionGC;
import static org.apache.jackrabbit.oak.plugins.document.FormatVersion.versionOf;
import static org.apache.jackrabbit.oak.plugins.document.util.Utils.getRootDocument;
import static org.apache.jackrabbit.oak.plugins.document.util.Utils.timestampToString;
import static org.apache.jackrabbit.oak.run.Utils.asCloseable;
import static org.apache.jackrabbit.oak.run.Utils.createDocumentMKBuilder;

/**
 * Gives information about current node revisions state.
 */
public class RevisionsCommand implements Command {

    private static final Logger LOG = LoggerFactory.getLogger(RevisionsCommand.class);

    private static final String USAGE = Joiner.on(System.lineSeparator()).join(
            "revisions {<jdbc-uri> | <mongodb-uri>} <sub-command> [options]",
            "where sub-command is one of",
            "  info     give information about the revisions state without performing",
            "           any modifications",
            "  collect  perform garbage collection",
            "  reset    clear all persisted metadata",
            "  sweep    clean up uncommitted changes"
    );

    private static final ImmutableList<String> LOGGER_NAMES = ImmutableList.of(
            "org.apache.jackrabbit.oak.plugins.document.VersionGarbageCollector",
            "org.apache.jackrabbit.oak.plugins.document.NodeDocumentSweeper"
    );

    private static class RevisionsOptions extends Utils.NodeStoreOptions {

        static final String CMD_INFO = "info";
        static final String CMD_COLLECT = "collect";
        static final String CMD_RESET = "reset";
        static final String CMD_SWEEP = "sweep";

        final OptionSpec<?> once;
        final OptionSpec<Integer> limit;
        final OptionSpec<Long> timeLimit;
        final OptionSpec<Long> olderThan;
        final OptionSpec<Double> delay;
        final OptionSpec<?> continuous;
        final OptionSpec<?> verbose;

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
            continuous = parser
                    .accepts("continuous", "run continuously (collect only)");
            verbose = parser
                    .accepts("verbose", "print INFO messages to the console");
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

        boolean isContinuous() {
            return options.has(continuous);
        }

        boolean isVerbose() {
            return options.has(verbose);
        }
    }

    @Override
    public void execute(String... args) throws Exception {
        Closer closer = Closer.create();
        try {
            RevisionsOptions options = new RevisionsOptions(USAGE).parse(args);
            setupLoggers(options.isVerbose());

            String subCmd = options.getSubCmd();
            if (RevisionsOptions.CMD_INFO.equals(subCmd)) {
                info(options, closer);
            } else if (RevisionsOptions.CMD_COLLECT.equals(subCmd)) {
                collect(options, closer);
            } else if (RevisionsOptions.CMD_RESET.equals(subCmd)) {
                reset(options, closer);
            } else if (RevisionsOptions.CMD_SWEEP.equals(subCmd)) {
                sweep(options, closer);
            } else {
                System.err.println("unknown revisions command: " + subCmd);
            }
        } catch (Throwable e) {
            LOG.error("Command failed", e);
            throw closer.rethrow(e);
        } finally {
            closer.close();
        }
    }

    private void setupLoggers(boolean verbose) {
        if (!verbose) {
            return;
        }
        LoggerContext ctxt = (LoggerContext) LoggerFactory.getILoggerFactory();
        for (String name : LOGGER_NAMES) {
            ctxt.getLogger(name).setLevel(Level.INFO);
        }
    }

    private VersionGarbageCollector bootstrapVGC(RevisionsOptions options,
                                                 Closer closer)
            throws IOException {
        DocumentNodeStoreBuilder<?> builder = createDocumentMKBuilder(options, closer);
        if (builder == null) {
            System.err.println("revisions mode only available for DocumentNodeStore");
            System.exit(1);
        }
        // create a VersionGCSupport while builder is read-write
        VersionGCSupport gcSupport = builder.createVersionGCSupport();
        // check for matching format version
        FormatVersion version = versionOf(gcSupport.getDocumentStore());
        if (!DocumentNodeStore.VERSION.equals(version)) {
            System.err.println("Incompatible versions. This oak-run is " +
                    DocumentNodeStore.VERSION + ", while the store is " +
                    version);
            System.exit(1);
        }
        // set it read-only before the DocumentNodeStore is created
        // this prevents the DocumentNodeStore from writing a new
        // clusterId to the clusterNodes and nodes collections
        builder.setReadOnlyMode();
        // create a version GC that operates on a read-only DocumentNodeStore
        // and a GC support with a writable DocumentStore
        VersionGarbageCollector gc = createVersionGC(builder.build(), gcSupport);

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

    private void info(RevisionsOptions options, Closer closer)
            throws IOException {
        VersionGarbageCollector gc = bootstrapVGC(options, closer);
        System.out.println("retrieving gc info");
        VersionGCInfo info = gc.getInfo(options.getOlderThan(), SECONDS);

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

    private void collect(final RevisionsOptions options, Closer closer)
            throws IOException {
        VersionGarbageCollector gc = bootstrapVGC(options, closer);
        ExecutorService executor = Executors.newSingleThreadExecutor();
        final Semaphore finished = new Semaphore(0);
        try {
            if (options.isContinuous()) {
                // collect until shutdown hook is called
                final AtomicBoolean running = new AtomicBoolean(true);
                Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
                    @Override
                    public void run() {
                        System.out.println("Detected QUIT signal.");
                        System.out.println("Stopping Revision GC...");
                        running.set(false);
                        finished.acquireUninterruptibly();
                        System.out.println("Stopped Revision GC.");
                    }
                }));
                while (running.get()) {
                    long lastRun = System.currentTimeMillis();
                    collectOnce(gc, options, executor);
                    waitWhile(running, lastRun + 5000);
                }
            } else {
                collectOnce(gc, options, executor);
            }
        } finally {
            finished.release();
            executor.shutdownNow();
        }
    }

    private void collectOnce(VersionGarbageCollector gc,
                             RevisionsOptions options,
                             ExecutorService executor) throws IOException {
        long started = System.currentTimeMillis();
        System.out.println("starting gc collect");
        Future<VersionGCStats> f = executor.submit(new Callable<VersionGCStats>() {
            @Override
            public VersionGCStats call() throws Exception {
                return gc.gc(options.getOlderThan(), SECONDS);
            }
        });
        if (options.getTimeLimit() >= 0) {
            try {
                f.get(options.getTimeLimit(), SECONDS);
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
    }

    private static void waitWhile(AtomicBoolean condition, long until) {
        long now = System.currentTimeMillis();
        while (now < until) {
            if (condition.get()) {
                try {
                    Thread.sleep(Math.min(1000, until - now));
                } catch (InterruptedException e) {
                    // ignore
                }
            }
            now = System.currentTimeMillis();
        }
    }

    private void reset(RevisionsOptions options, Closer closer)
            throws IOException {
        VersionGarbageCollector gc = bootstrapVGC(options, closer);
        System.out.println("resetting recommendations and statistics");
        gc.reset();
    }

    private void sweep(RevisionsOptions options, Closer closer)
            throws IOException {
        int clusterId = options.getClusterId();
        if (clusterId <= 0) {
            System.err.println("clusterId option is required for " +
                    RevisionsOptions.CMD_SWEEP + " command");
            return;
        }
        DocumentNodeStoreBuilder<?> builder = createDocumentMKBuilder(options, closer);
        if (builder == null) {
            System.err.println("revisions mode only available for DocumentNodeStore");
            return;
        }
        // usage of this DocumentNodeStore is single threaded. Reduce the
        // number of cache segments to a minimum. This allows for caching
        // bigger entries that would otherwise be evicted immediately
        builder.setCacheSegmentCount(1);
        DocumentStore store = builder.getDocumentStore();
        // cluster node must be inactive
        for (ClusterNodeInfoDocument doc : ClusterNodeInfoDocument.all(store)) {
            if (doc.getClusterId() == clusterId && doc.isActive()) {
                System.err.println("cannot sweep revisions for active " +
                        "clusterId " + clusterId);
                return;
            }
        }
        // the root document must have a _lastRev entry for the clusterId
        if (!getRootDocument(store).getLastRev().containsKey(clusterId)) {
            System.err.println("store does not have changes with " +
                    "clusterId " + clusterId);
            return;
        }
        builder.setReadOnlyMode();
        DocumentNodeStore ns = builder.build();
        closer.register(asCloseable(ns));
        MissingLastRevSeeker seeker = builder.createMissingLastRevSeeker();
        SweepHelper.sweep(store, new RevisionContextWrapper(ns, clusterId), seeker);
    }

    private String fmtTimestamp(long ts) {
        return timestampToString(ts);
    }

    private String fmtDuration(long ts) {
        return TimeDurationFormatter.forLogging().format(ts, TimeUnit.MILLISECONDS);
    }
}
