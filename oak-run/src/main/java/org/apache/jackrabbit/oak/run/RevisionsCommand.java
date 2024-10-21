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

import org.apache.jackrabbit.guava.common.base.Joiner;
import org.apache.jackrabbit.guava.common.io.Closer;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
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
import org.apache.jackrabbit.oak.plugins.document.NodeDocument;
import org.apache.jackrabbit.oak.plugins.document.RevisionContextWrapper;
import org.apache.jackrabbit.oak.plugins.document.SweepHelper;
import org.apache.jackrabbit.oak.plugins.document.VersionGCSupport;
import org.apache.jackrabbit.oak.plugins.document.VersionGarbageCollector.VersionGCInfo;
import org.apache.jackrabbit.oak.plugins.document.VersionGarbageCollector.VersionGCStats;
import org.apache.jackrabbit.oak.plugins.document.util.MongoConnection;
import org.apache.jackrabbit.oak.run.commons.Command;
import org.apache.jackrabbit.oak.plugins.document.VersionGCOptions;
import org.apache.jackrabbit.oak.plugins.document.VersionGarbageCollector;
import org.apache.jackrabbit.oak.spi.blob.MemoryBlobStore;
import org.apache.jackrabbit.oak.spi.gc.LoggingGCMonitor;
import org.apache.jackrabbit.oak.stats.DefaultStatisticsProvider;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.lang.Boolean.FALSE;
import static java.lang.Boolean.TRUE;
import static java.lang.System.currentTimeMillis;
import static java.util.List.of;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.commons.lang3.StringUtils.isNotEmpty;
import static org.apache.jackrabbit.oak.plugins.document.Collection.NODES;
import static org.apache.jackrabbit.oak.plugins.document.DocumentNodeStoreHelper.createVersionGC;
import static org.apache.jackrabbit.oak.plugins.document.FormatVersion.versionOf;
import static org.apache.jackrabbit.oak.plugins.document.util.Utils.getIdFromPath;
import static org.apache.jackrabbit.oak.plugins.document.util.Utils.getRootDocument;
import static org.apache.jackrabbit.oak.plugins.document.util.Utils.timestampToString;
import static org.apache.jackrabbit.oak.run.Utils.asCloseable;
import static org.apache.jackrabbit.oak.run.Utils.createDocumentMKBuilder;
import static org.apache.jackrabbit.oak.run.Utils.getMongoConnection;

/**
 * Gives information about current node revisions state.
 */
public class RevisionsCommand implements Command {

    private static final Logger LOG = LoggerFactory.getLogger(RevisionsCommand.class);

    private static final String USAGE = Joiner.on(System.lineSeparator()).join(
            "revisions {<jdbc-uri> | <mongodb-uri>} <sub-command> [options]",
            "where sub-command is one of",
            "  info           give information about the revisions state without performing",
            "                 any modifications",
            "  collect        perform garbage collection",
            "  reset          clear all persisted metadata",
            "  sweep          clean up uncommitted changes",
            "  fullGC         perform full garbage collection i.e. remove unmerged branch commits, old ",
            "                 revisions, deleted properties etc. Use the --entireRepo to run it on the entire",
            "                 repository, or --path argument to perform the fullGC only on the specific document"
    );

    private static final List<String> LOGGER_NAMES = of(
            "org.apache.jackrabbit.oak.plugins.document.VersionGarbageCollector",
            "org.apache.jackrabbit.oak.plugins.document.NodeDocumentSweeper",
            "org.apache.jackrabbit.oak.plugins.document.VersionGarbageCollector.auditFGC",
            "org.apache.jackrabbit.oak.plugins.document.mongo.MongoVersionGCSupport",
            "org.apache.jackrabbit.oak.plugins.document.VersionGCRecommendations"
    );

    private static class RevisionsOptions extends Utils.NodeStoreOptions {

        static final String CMD_INFO = "info";
        static final String CMD_COLLECT = "collect";
        static final String CMD_RESET = "reset";
        static final String CMD_SWEEP = "sweep";
        static final String CMD_FULL_GC = "fullGC";

        final OptionSpec<?> once;
        final OptionSpec<Integer> limit;
        final OptionSpec<Integer> fullGcBatchSize;
        final OptionSpec<Integer> fullGcProgressSize;
        final OptionSpec<Long> timeLimit;
        final OptionSpec<Long> olderThan;
        final OptionSpec<Double> delay;
        final OptionSpec<Double> fullGcDelayFactor;
        final OptionSpec<?> continuous;
        final OptionSpec<?> fullGCOnly;
        final OptionSpec<Boolean> resetFullGC;
        final OptionSpec<?> verbose;
        final OptionSpec<String> path;
        final OptionSpec<String> includePaths;
        final OptionSpec<String> excludePaths;
        final OptionSpec<?> entireRepo;
        final OptionSpec<?> compact;
        final OptionSpec<Boolean> dryRun;
        final OptionSpec<Boolean> embeddedVerification;
        final OptionSpec<Integer> fullGcMode;

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
            fullGcDelayFactor = parser
                    .accepts("fullGcDelayFactor", "introduce delays after each fullGc batch to reduce load " +
                            "on system")
                    .withRequiredArg().ofType(Double.class).defaultsTo(2.0);
            timeLimit = parser
                    .accepts("timeLimit", "cancel garbage collection after n seconds").withRequiredArg()
                    .ofType(Long.class).defaultsTo(-1L);
            dryRun = parser.accepts("dryRun", "dryRun of fullGC i.e. only print what would be deleted")
                    .withRequiredArg().ofType(Boolean.class).defaultsTo(TRUE);
            embeddedVerification = parser.accepts("verify", "enable embedded verification of fullGC " +
                            "during dryRun mode i.e. will verify the effect of fullGC operation on each document after " +
                            "applying the changes in memory and will raise flag if it can cause issues")
                    .withRequiredArg().ofType(Boolean.class).defaultsTo(TRUE);
            fullGcMode = parser.accepts("fullGcMode", "Mode of fullGC")
                    .withRequiredArg().ofType(Integer.class).defaultsTo(0);
            continuous = parser
                    .accepts("continuous", "run continuously (collect only)");
            fullGCOnly = parser
                    .accepts("fullGCOnly", "apply only to fullGC (reset only)");
            verbose = parser
                    .accepts("verbose", "print INFO messages to the console");
            path = parser
                    .accepts("path", "path to the document to be cleaned up").withRequiredArg();
            includePaths = parser
                    .accepts("includePaths", "Paths which should be included in fullGC. " +
                            "Paths should be separated with '::'. Example: --includePaths=/content::/var")
                    .withRequiredArg().ofType(String.class).defaultsTo("/");
            excludePaths = parser
                    .accepts("excludePaths", "Paths which should be excluded from fullGC. " +
                            "Paths should be separated with '::'. Example: --excludePaths=/content::/var")
                    .withRequiredArg().ofType(String.class).defaultsTo("");
            entireRepo = parser
                    .accepts("entireRepo", "run fullGC on the entire repository");
            compact = parser
                    .accepts("compact", "run compaction on document store (only mongo) after running fullGC");
            resetFullGC = parser
                    .accepts("resetFullGC", "reset fullGC after running FullGC")
                    .withRequiredArg().ofType(Boolean.class).defaultsTo(FALSE);
            fullGcBatchSize = parser.accepts("fullGcBatchSize", "The number of documents to fetch from database " +
                            "in a single query to check for Full GC.")
                    .withRequiredArg().ofType(Integer.class).defaultsTo(1000);
            fullGcProgressSize = parser.accepts("fullGcProgressSize", "The number of documents to check for " +
                            "garbage in each Full GC cycle")
                    .withRequiredArg().ofType(Integer.class).defaultsTo(10000);
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

        boolean isDryRun() {
            return dryRun.value(options);
        }

        boolean isEmbeddedVerificationEnabled() {
            return embeddedVerification.value(options);
        }

        int getFullGcMode() {
            return fullGcMode.value(options);
        }

        int getFullGcBatchSize() {
            return fullGcBatchSize.value(options);
        }

        int getFullGcProgressSize() {
            return fullGcProgressSize.value(options);
        }

        double getFullGcDelayFactor() {
            return fullGcDelayFactor.value(options);
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

        String getPath() {
            return path.value(options);
        }

        String[] getIncludePaths() {
            return includePaths.value(options).split("::");
        }

        String[] getExcludePaths() {
            return excludePaths.value(options).split("::");
        }

        boolean isResetFullGCOnly() {
            return options.has(fullGCOnly);
        }

        boolean isResetFullGC() {
            return resetFullGC.value(options);
        }

        boolean isVerbose() {
            return options.has(verbose);
        }

        boolean isEntireRepo() {
            return options.has(entireRepo);
        }

        boolean doCompaction() {
            return options.has(compact);
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
                collect(options, closer, false);
            } else if (RevisionsOptions.CMD_RESET.equals(subCmd)) {
                reset(options, closer);
            } else if (RevisionsOptions.CMD_SWEEP.equals(subCmd)) {
                sweep(options, closer);
            } else if (RevisionsOptions.CMD_FULL_GC.equals(subCmd)) {
                boolean entireRepo = options.isEntireRepo();
                String path = options.getPath();
                if (entireRepo) {
                    collect(options, closer, true);
                } else if (isNotEmpty(path)) {
                    collectDocument(options, closer, path);
                } else {
                    System.err.println("--path or --entireRepo option is required for " +
                        RevisionsOptions.CMD_FULL_GC + " command");
                }
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

    private VersionGarbageCollector bootstrapVGC(RevisionsOptions options, Closer closer, boolean fullGCEnabled) throws IOException {
        DocumentNodeStoreBuilder<?> builder = createDocumentMKBuilder(options, closer);
        if (builder == null) {
            System.err.println("revisions mode only available for DocumentNodeStore");
            System.exit(1);
        }
        // set fullGC
        builder.setFullGCEnabled(fullGCEnabled);
        builder.setFullGCIncludePaths(options.getIncludePaths());
        builder.setFullGCExcludePaths(options.getExcludePaths());
        builder.setFullGCMode(options.getFullGcMode());
        builder.setFullGCDelayFactor(options.getFullGcDelayFactor());
        builder.setFullGCBatchSize(options.getFullGcBatchSize());
        builder.setFullGCProgressSize(options.getFullGcProgressSize());

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
        if (options.isEmbeddedVerificationEnabled()) {
            builder.setEmbeddedVerificationEnabled(true);
        }
        // set it read-only before the DocumentNodeStore is created
        // this prevents the DocumentNodeStore from writing a new
        // clusterId to the clusterNodes and nodes collections
        builder.setReadOnlyMode();
        useMemoryBlobStore(builder);
        // create a version GC that operates on a read-only DocumentNodeStore
        // and a GC support with a writable DocumentStore
        System.out.println("DryRun is enabled : " + options.isDryRun());
        System.out.println("EmbeddedVerification is enabled : " + options.isEmbeddedVerificationEnabled());
        System.out.println("ResetFullGC is enabled : " + options.isResetFullGC());
        System.out.println("Compaction is enabled : " + options.doCompaction());
        System.out.println("IncludePaths are : " + Arrays.toString(options.getIncludePaths()));
        System.out.println("ExcludePaths are : " + Arrays.toString(options.getExcludePaths()));
        System.out.println("FullGcMode is : " + options.getFullGcMode());
        System.out.println("FullGcDelayFactory is : " + options.getFullGcDelayFactor());
        System.out.println("FullGcBatchSize is : " + options.getFullGcBatchSize());
        System.out.println("FullGcProgressSize is : " + options.getFullGcProgressSize());
        VersionGarbageCollector gc = createVersionGC(builder.build(), gcSupport, options.isDryRun(), builder);

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

    private void info(RevisionsOptions options, Closer closer) throws IOException {
        VersionGarbageCollector gc = bootstrapVGC(options, closer, false);
        System.out.println("retrieving gc info");
        printInfo(gc, options);
    }

    private void printInfo(VersionGarbageCollector gc, RevisionsOptions options) throws IOException {
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
        System.out.printf(Locale.US, "%21s  %s%n", "Oldest FullGC:",
                fmtTimestamp(info.oldestFullGCRevisionEstimate));
    }

    private void collect(final RevisionsOptions options, Closer closer, boolean fullGCEnabled) throws IOException {
        VersionGarbageCollector gc = bootstrapVGC(options, closer, fullGCEnabled);
        // Set a default statistics provider
        gc.setStatisticsProvider(new DefaultStatisticsProvider(Executors.newSingleThreadScheduledExecutor()));
        ExecutorService executor = Executors.newSingleThreadExecutor();
        final Semaphore finished = new Semaphore(0);
        try {
            // collect until shutdown hook is called
            final AtomicBoolean running = new AtomicBoolean(true);
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                System.out.println("Detected QUIT signal.");
                System.out.println("Stopping Revision GC...");
                running.set(false);
                gc.cancel();
                finished.acquireUninterruptibly();
                System.out.println("Stopped Revision GC.");
                if (fullGCEnabled) {
                    System.out.println("Full GC Stats:");
                    System.out.println("    " + gc.getFullGCStatsReport());
                }
            }));
            if (options.isContinuous()) {
                while (running.get()) {
                    long lastRun = System.currentTimeMillis();
                    collectOnce(gc, options, executor);
                    waitWhile(running, lastRun + 5000);
                }
            } else {
                collectOnce(gc, options, executor);
            }
            System.out.println("retrieving gc info");
            printInfo(gc, options);
        } finally {
            finished.release();
            if (options.isDryRun()) {
                gc.resetDryRun();
            }
            if (options.isResetFullGC()) {
                gc.resetFullGC();
            }
            if (options.doCompaction()) {
                Optional<MongoConnection> mongoClient = getMongoConnection(options, closer);
                final long start = currentTimeMillis();
                mongoClient.ifPresentOrElse(
                        con -> {
                            Document compact = con.getDatabase().runCommand(new Document("compact", NODES.toString()));
                            System.out.format("Compact done in %s ms, Output is %s", (currentTimeMillis() - start),  compact);
                        },
                        () -> System.err.println("Database is null"));
            }
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
        VersionGarbageCollector gc = bootstrapVGC(options, closer, false);
        System.out.println("resetting recommendations and statistics");
        if (options.isResetFullGCOnly()) {
            gc.resetFullGC();
        } else {
            gc.reset();
        }
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
        useMemoryBlobStore(builder);
        DocumentNodeStore ns = builder.build();
        closer.register(asCloseable(ns));
        MissingLastRevSeeker seeker = builder.createMissingLastRevSeeker();
        SweepHelper.sweep(store, new RevisionContextWrapper(ns, clusterId), seeker);
    }

    private void collectDocument(RevisionsOptions options, Closer closer, String path) throws IOException {
        DocumentNodeStoreBuilder<?> builder = createDocumentMKBuilder(options, closer);
        if (builder == null) {
            System.err.println("revisions mode only available for DocumentNodeStore");
            return;
        }

        System.out.println("running fullGC on the document: " + path);
        DocumentStore documentStore = builder.getDocumentStore();
        builder.setReadOnlyMode();
        useMemoryBlobStore(builder);
        DocumentNodeStore documentNodeStore = builder.build();

        VersionGarbageCollector gc = bootstrapVGC(options, closer, true);
        // Set a LoggingGC monitor that will output the fullGC operations
        gc.setGCMonitor(new LoggingGCMonitor(LOG));
        // Set a default statistics provider
        gc.setStatisticsProvider(new DefaultStatisticsProvider(Executors.newSingleThreadScheduledExecutor()));
        // Run fullGC on the given document
        NodeDocument workingDocument = documentStore.find(NODES, getIdFromPath(path));
        if (workingDocument == null) {
            System.err.println("Document not found: " + path);
            return;
        }
        gc.collectGarbageOnDocument(documentNodeStore, workingDocument, options.isVerbose());

        System.out.println("Full GC Stats:");
        System.out.println("    " + gc.getFullGCStatsReport());
    }

    private String fmtTimestamp(long ts) {
        return timestampToString(ts);
    }

    private String fmtDuration(long ts) {
        return TimeDurationFormatter.forLogging().format(ts, TimeUnit.MILLISECONDS);
    }

    private void useMemoryBlobStore(DocumentNodeStoreBuilder builder) {
        // The revisions command does not have options for the blob store
        // and the DocumentNodeStoreBuilder by default assumes the blobs
        // are stored in the same location as the documents. That is,
        // either in MongoDB or RDB, which is not necessarily the case and
        // can cause an exception when the blob store implementation starts
        // read-only on a database that does not have the required
        // collection. Use an in-memory blob store instead, because the
        // revisions command does not read blobs anyway.
        builder.setBlobStore(new MemoryBlobStore());
    }
}
