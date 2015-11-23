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

import static com.google.common.collect.Sets.newHashSet;
import static java.util.Arrays.asList;
import static org.apache.commons.io.FileUtils.byteCountToDisplaySize;
import static org.apache.jackrabbit.oak.checkpoint.Checkpoints.CP;
import static org.apache.jackrabbit.oak.plugins.segment.RecordType.NODE;
import static org.apache.jackrabbit.oak.plugins.segment.file.FileStore.newFileStore;
import static org.apache.jackrabbit.oak.plugins.segment.file.tooling.ConsistencyChecker.checkConsistency;
import static org.slf4j.LoggerFactory.getLogger;

import java.io.Closeable;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Queue;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.jcr.Repository;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import com.google.common.base.Joiner;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Maps;
import com.google.common.collect.Queues;
import com.google.common.io.Closer;
import com.google.common.util.concurrent.AbstractScheduledService;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.MongoURI;
import joptsimple.ArgumentAcceptingOptionSpec;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import org.apache.commons.io.FileUtils;
import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.api.ContentRepository;
import org.apache.jackrabbit.oak.benchmark.BenchmarkRunner;
import org.apache.jackrabbit.oak.checkpoint.Checkpoints;
import org.apache.jackrabbit.oak.commons.IOUtils;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.commons.json.JsopBuilder;
import org.apache.jackrabbit.oak.console.Console;
import org.apache.jackrabbit.oak.explorer.Explorer;
import org.apache.jackrabbit.oak.explorer.NodeStoreTree;
import org.apache.jackrabbit.oak.fixture.OakFixture;
import org.apache.jackrabbit.oak.http.OakServlet;
import org.apache.jackrabbit.oak.jcr.Jcr;
import org.apache.jackrabbit.oak.json.JsopDiff;
import org.apache.jackrabbit.oak.plugins.backup.FileStoreBackup;
import org.apache.jackrabbit.oak.plugins.backup.FileStoreRestore;
import org.apache.jackrabbit.oak.plugins.document.DocumentMK;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeStore;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeStoreHelper;
import org.apache.jackrabbit.oak.plugins.document.LastRevRecoveryAgent;
import org.apache.jackrabbit.oak.plugins.document.NodeDocument;
import org.apache.jackrabbit.oak.plugins.document.mongo.MongoDocumentStore;
import org.apache.jackrabbit.oak.plugins.document.mongo.MongoDocumentStoreHelper;
import org.apache.jackrabbit.oak.plugins.document.mongo.MongoMissingLastRevSeeker;
import org.apache.jackrabbit.oak.plugins.document.util.CloseableIterable;
import org.apache.jackrabbit.oak.plugins.document.util.MapDBMapFactory;
import org.apache.jackrabbit.oak.plugins.document.util.MapFactory;
import org.apache.jackrabbit.oak.plugins.document.util.MongoConnection;
import org.apache.jackrabbit.oak.plugins.segment.FileStoreDiff;
import org.apache.jackrabbit.oak.plugins.segment.FileStoreHelper;
import org.apache.jackrabbit.oak.plugins.segment.PCMAnalyser;
import org.apache.jackrabbit.oak.plugins.segment.RecordId;
import org.apache.jackrabbit.oak.plugins.segment.RecordUsageAnalyser;
import org.apache.jackrabbit.oak.plugins.segment.Segment;
import org.apache.jackrabbit.oak.plugins.segment.SegmentId;
import org.apache.jackrabbit.oak.plugins.segment.SegmentNodeState;
import org.apache.jackrabbit.oak.plugins.segment.SegmentNodeStore;
import org.apache.jackrabbit.oak.plugins.segment.SegmentTracker;
import org.apache.jackrabbit.oak.plugins.segment.compaction.CompactionStrategy;
import org.apache.jackrabbit.oak.plugins.segment.compaction.CompactionStrategy.CleanupType;
import org.apache.jackrabbit.oak.plugins.segment.file.FileStore;
import org.apache.jackrabbit.oak.plugins.segment.file.FileStore.ReadOnlyStore;
import org.apache.jackrabbit.oak.plugins.segment.file.JournalReader;
import org.apache.jackrabbit.oak.plugins.segment.standby.client.StandbyClient;
import org.apache.jackrabbit.oak.plugins.segment.standby.server.StandbyServer;
import org.apache.jackrabbit.oak.plugins.tika.TextExtractorMain;
import org.apache.jackrabbit.oak.remote.content.ContentRemoteRepository;
import org.apache.jackrabbit.oak.remote.http.RemoteServlet;
import org.apache.jackrabbit.oak.scalability.ScalabilityRunner;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.server.remoting.davex.JcrRemotingServlet;
import org.apache.jackrabbit.webdav.jcr.JCRWebdavServerServlet;
import org.apache.jackrabbit.webdav.server.AbstractWebdavServlet;
import org.apache.jackrabbit.webdav.simple.SimpleWebdavServlet;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;

public final class Main {

    public static final int PORT = 8080;
    public static final String URI = "http://localhost:" + PORT + "/";

    private static final int MB = 1024 * 1024;

    public static final boolean TAR_STORAGE_MEMORY_MAPPED = Boolean.getBoolean("tar.memoryMapped");

    private Main() {
    }

    public static void main(String[] args) throws Exception {
        printProductInfo(args);

        Mode mode = Mode.SERVER;
        if (args.length > 0) {
            try {
                mode = Mode.valueOf(args[0].toUpperCase(Locale.ENGLISH));
            } catch (IllegalArgumentException e) {
                System.err.println("Unknown run mode: " + args[0]);
                mode = Mode.HELP;
            }
            String[] tail = new String[args.length - 1];
            System.arraycopy(args, 1, tail, 0, tail.length);
            args = tail;
        }
        switch (mode) {
            case BACKUP:
                backup(args);
                break;
            case RESTORE:
                restore(args);
                break;
            case BENCHMARK:
                BenchmarkRunner.main(args);
                break;
            case CONSOLE:
                Console.main(args);
                break;
            case DEBUG:
                debug(args);
                break;
            case GRAPH:
                graph(args);
                break;
            case CHECK:
                check(args);
                break;
            case COMPACT:
                compact(args);
                break;
            case SERVER:
                server(URI, args);
                break;
            case UPGRADE:
                System.out.println("This command was moved to the oak-upgrade module");
                break;
            case SCALABILITY:
                ScalabilityRunner.main(args);
                break;
            case EXPLORE:
                Explorer.main(args);
                break;
            case STANDBY:
                standbyInstance(args);
                break;
            case PRIMARY:
                primaryInstance(args);
                break;
            case CHECKPOINTS:
                checkpoints(args);
                break;
            case RECOVERY:
                recovery(args);
                break;
            case REPAIR:
                repair(args);
                break;
            case TIKA:
                TextExtractorMain.main(args);
                break;
            case GARBAGE:
                garbage(args);
                break;
            case TARMKDIFF:
                FileStoreDiff.main(args);
                break;
            case HELP:
            default:
                System.err.print("Available run modes: ");
                System.err.println(Joiner.on(',').join(Mode.values()));
                System.exit(1);

        }
    }

    public static String getProductInfo(){
        String version = null;

        try {
            InputStream stream = Main.class
                    .getResourceAsStream("/META-INF/maven/org.apache.jackrabbit/oak-run/pom.properties");
            if (stream != null) {
                try {
                    Properties properties = new Properties();
                    properties.load(stream);
                    version = properties.getProperty("version");
                } finally {
                    stream.close();
                }
            }
        } catch (Exception ignore) {
        }

        String product;
        if (version != null) {
            product = "Apache Jackrabbit Oak " + version;
        } else {
            product = "Apache Jackrabbit Oak";
        }

        return product;
    }

    private static void printProductInfo(String[] args) {
        if(!Arrays.asList(args).contains("--quiet")) {
            System.out.println(getProductInfo());
        }
    }

    private static void backup(String[] args) throws IOException {
        Closer closer = Closer.create();
        String h = "backup { /path/to/oak/repository | mongodb://host:port/database } <path/to/backup>";
        try {
            NodeStore store = bootstrapNodeStore(args, closer, h);
            FileStoreBackup.backup(store, new File(args[1]));
        } catch (Throwable e) {
            throw closer.rethrow(e);
        } finally {
            closer.close();
        }
    }

    private static void restore(String[] args) throws IOException {
        Closer closer = Closer.create();
        String h = "restore { /path/to/oak/repository | mongodb://host:port/database } <path/to/backup>";
        try {
            NodeStore store = bootstrapNodeStore(args, closer, h);
            FileStoreRestore.restore(new File(args[1]), store);
        } catch (Throwable e) {
            throw closer.rethrow(e);
        } finally {
            closer.close();
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


    private static void standbyInstance(String[] args) throws Exception {

        final String defaultHost = "127.0.0.1";
        final int defaultPort = 8023;

        final OptionParser parser = new OptionParser();
        final OptionSpec<String> host = parser.accepts("host", "master host").withRequiredArg().ofType(String.class).defaultsTo(defaultHost);
        final OptionSpec<Integer> port = parser.accepts("port", "master port").withRequiredArg().ofType(Integer.class).defaultsTo(defaultPort);
        final OptionSpec<Integer> interval = parser.accepts("interval", "interval between successive executions").withRequiredArg().ofType(Integer.class);
        final OptionSpec<Boolean> secure = parser.accepts("secure", "use secure connections").withRequiredArg().ofType(Boolean.class);
        final OptionSpec<?> help = parser.acceptsAll(asList("h", "?", "help"), "show help").forHelp();
        final OptionSpec<String> nonOption = parser.nonOptions(Mode.STANDBY + " <path to repository>");

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
            store = new FileStore(new File(nonOptions.get(0)), 256);
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

    private static void primaryInstance(String[] args) throws Exception {

        final int defaultPort = 8023;

        final OptionParser parser = new OptionParser();
        final OptionSpec<Integer> port = parser.accepts("port", "port to listen").withRequiredArg().ofType(Integer.class).defaultsTo(defaultPort);
        final OptionSpec<Boolean> secure = parser.accepts("secure", "use secure connections").withRequiredArg().ofType(Boolean.class);
        final OptionSpec<String> admissible = parser.accepts("admissible", "list of admissible slave host names or ip ranges").withRequiredArg().ofType(String.class);
        final OptionSpec<?> help = parser.acceptsAll(asList("h", "?", "help"), "show help").forHelp();
        final OptionSpec<String> nonOption = parser.nonOptions(Mode.PRIMARY + " <path to repository>");

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
            store = new FileStore(new File(nonOptions.get(0)), 256);
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

    public static NodeStore bootstrapNodeStore(String[] args, Closer closer,
            String h) throws IOException {
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
        FileStore fs = new FileStore(new File(src), 256, TAR_STORAGE_MEMORY_MAPPED);
        closer.register(asCloseable(fs));
        return new SegmentNodeStore(fs);
    }

    private static Closeable asCloseable(final FileStore fs) {
        return new Closeable() {

            @Override
            public void close() throws IOException {
                fs.close();
            }
        };
    }

    private static Closeable asCloseable(final DocumentNodeStore dns) {
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

    private static void compact(String[] args) throws IOException {
        if (args.length != 1) {
            System.err.println("usage: compact <path>");
            System.exit(1);
        } else if (!isValidFileStore(args[0])) {
            System.err.println("Invalid FileStore directory " + args[0]);
            System.exit(1);
        } else {
            boolean persistCM = Boolean
                    .getBoolean("tar.PersistCompactionMap");
            Stopwatch watch = Stopwatch.createStarted();
            File directory = new File(args[0]);
            System.out.println("Compacting " + directory);
            System.out.println("    before " + Arrays.toString(directory.list()));
            long sizeBefore = FileUtils.sizeOfDirectory(directory);
            System.out.println("    size "
                    + IOUtils.humanReadableByteCount(sizeBefore) + " (" + sizeBefore
                    + " bytes)");

            System.out.println("    -> compacting");
            FileStore store = openFileStore(directory);
            try {
                CompactionStrategy compactionStrategy = new CompactionStrategy(
                        false, CompactionStrategy.CLONE_BINARIES_DEFAULT,
                        CleanupType.CLEAN_ALL, 0,
                        CompactionStrategy.MEMORY_THRESHOLD_DEFAULT) {
                    @Override
                    public boolean compacted(Callable<Boolean> setHead)
                            throws Exception {
                        // oak-run is doing compaction single-threaded
                        // hence no guarding needed - go straight ahead
                        // and call setHead
                        return setHead.call();
                    }
                };
                compactionStrategy.setOfflineCompaction(true);
                compactionStrategy.setPersistCompactionMap(persistCM);
                store.setCompactionStrategy(compactionStrategy);
                store.compact();
            } finally {
                store.close();
            }

            System.out.println("    -> cleaning up");
            store = openFileStore(directory);
            try {
                store.cleanup();

                String head;
                File journal = new File(directory, "journal.log");
                JournalReader journalReader = new JournalReader(journal);
                try {
                    head = journalReader.iterator().next() + " root\n";
                } finally {
                    journalReader.close();
                }

                RandomAccessFile journalFile = new RandomAccessFile(journal, "rw");
                try {
                    System.out.println("    -> writing new " + journal.getName() + ": " + head);
                    journalFile.setLength(0);
                    journalFile.writeBytes(head);
                    journalFile.getChannel().force(false);
                } finally {
                    journalFile.close();
                }
            } finally {
                store.close();
            }
            watch.stop();
            System.out.println("    after  "
                    + Arrays.toString(directory.list()));
            long sizeAfter = FileUtils.sizeOfDirectory(directory);
            System.out.println("    size "
                    + IOUtils.humanReadableByteCount(sizeAfter) + " (" + sizeAfter
                    + " bytes)");
            System.out.println("    duration  " + watch.toString() + " ("
                    + watch.elapsed(TimeUnit.SECONDS) + "s).");
        }
    }

    private static FileStore openFileStore(File directory) throws IOException {
        return newFileStore(directory)
                .withCacheSize(256)
                .withMemoryMapping(TAR_STORAGE_MEMORY_MAPPED)
                .create();
    }

    private static void checkpoints(String[] args) throws IOException {
        if (args.length == 0) {
            System.out
                    .println("usage: checkpoints {<path>|<mongo-uri>} [list|rm-all|rm-unreferenced|rm <checkpoint>]");
            System.exit(1);
        }
        boolean success = false;
        Checkpoints cps;
        Closer closer = Closer.create();
        try {
            String op = "list";
            if (args.length >= 2) {
                op = args[1];
                if (!"list".equals(op) && !"rm-all".equals(op) && !"rm-unreferenced".equals(op) && !"rm".equals(op)) {
                    failWith("Unknown command.");
                }
            }

            if (args[0].startsWith(MongoURI.MONGODB_PREFIX)) {
                MongoClientURI uri = new MongoClientURI(args[0]);
                MongoClient client = new MongoClient(uri);
                final DocumentNodeStore store = new DocumentMK.Builder()
                        .setMongoDB(client.getDB(uri.getDatabase()))
                        .getNodeStore();
                closer.register(new Closeable() {
                    @Override
                    public void close() throws IOException {
                        store.dispose();
                    }
                });
                cps = Checkpoints.onDocumentMK(store);
            } else if (isValidFileStore(args[0])) {
                final FileStore store = new FileStore(new File(args[0]),
                        256, TAR_STORAGE_MEMORY_MAPPED);
                closer.register(new Closeable() {
                    @Override
                    public void close() throws IOException {
                        store.close();
                    }
                });
                cps = Checkpoints.onTarMK(store);
            } else {
                failWith("Invalid FileStore directory " + args[0]);
                return;
            }

            System.out.println("Checkpoints " + args[0]);
            if ("list".equals(op)) {
                int cnt = 0;
                for (CP cp : cps.list()) {
                    System.out.printf("- %s created %s expires %s%n",
                            cp.id,
                            new Timestamp(cp.created),
                            new Timestamp(cp.expires));
                    cnt++;
                }
                System.out.println("Found " + cnt + " checkpoints");
            }
            if ("rm-all".equals(op)) {
                long time = System.currentTimeMillis();
                long cnt = cps.removeAll();
                time = System.currentTimeMillis() - time;
                if (cnt != -1) {
                    System.out.println("Removed " + cnt + " checkpoints in " + time + "ms.");
                } else {
                    failWith("Failed to remove all checkpoints.");
                }
            }
            if ("rm-unreferenced".equals(op)) {
                long time = System.currentTimeMillis();
                long cnt = cps.removeUnreferenced();
                time = System.currentTimeMillis() - time;
                if (cnt != -1) {
                    System.out.println("Removed " + cnt + " checkpoints in " + time + "ms.");
                } else {
                    failWith("Failed to remove unreferenced checkpoints.");
                }
            }
            if ("rm".equals(op)) {
                if (args.length != 3) {
                    failWith("Missing checkpoint id");
                } else {
                    String cp = args[2];
                    long time = System.currentTimeMillis();
                    int cnt = cps.remove(cp);
                    time = System.currentTimeMillis() - time;
                    if (cnt != 0) {
                        if (cnt == 1) {
                            System.out.println("Removed checkpoint " + cp + " in "
                                    + time + "ms.");
                        } else {
                            failWith("Failed to remove checkpoint " + cp);
                        }
                    } else {
                        failWith("Checkpoint '" + cp + "' not found.");
                    }
                }
            }
            success = true;
        } catch (Throwable t) {
            System.err.println(t.getMessage());
        } finally {
            closer.close();
        }
        if (!success) {
            System.exit(1);
        }
    }

    private static void failWith(String message) {
        throw new RuntimeException(message);
    }

    private static void recovery(String[] args) throws IOException {
        MapFactory.setInstance(new MapDBMapFactory());
        Closer closer = Closer.create();
        String h = "recovery mongodb://host:port/database { dryRun }";
        try {
            NodeStore store = bootstrapNodeStore(args, closer, h);
            if (!(store instanceof DocumentNodeStore)) {
                System.err.println("Recovery only available for DocumentNodeStore");
                System.exit(1);
            }
            DocumentNodeStore dns = (DocumentNodeStore) store;
            if (!(dns.getDocumentStore() instanceof MongoDocumentStore)) {
                System.err.println("Recovery only available for MongoDocumentStore");
                System.exit(1);
            }
            MongoDocumentStore docStore = (MongoDocumentStore) dns.getDocumentStore();
            LastRevRecoveryAgent agent = new LastRevRecoveryAgent(dns);
            MongoMissingLastRevSeeker seeker = new MongoMissingLastRevSeeker(docStore);
            CloseableIterable<NodeDocument> docs = seeker.getCandidates(0);
            closer.register(docs);
            boolean dryRun = Arrays.asList(args).contains("dryRun");
            agent.recover(docs.iterator(), dns.getClusterId(), dryRun);
        } catch (Throwable e) {
            throw closer.rethrow(e);
        } finally {
            closer.close();
        }
    }
    
    private static void repair(String[] args) throws IOException {
        Closer closer = Closer.create();
        String h = "repair mongodb://host:port/database path";
        try {
            NodeStore store = bootstrapNodeStore(args, closer, h);
            if (!(store instanceof DocumentNodeStore)) {
                System.err.println("Repair only available for DocumentNodeStore");
                System.exit(1);
            }
            DocumentNodeStore dns = (DocumentNodeStore) store;
            if (!(dns.getDocumentStore() instanceof MongoDocumentStore)) {
                System.err.println("Repair only available for MongoDocumentStore");
                System.exit(1);
            }
            MongoDocumentStore docStore = (MongoDocumentStore) dns.getDocumentStore();

            String path = args[args.length - 1];
            MongoDocumentStoreHelper.repair(docStore, path);
        } catch (Throwable e) {
            throw closer.rethrow(e);
        } finally {
            closer.close();
        }
    }

    private static void garbage(String[] args) throws IOException {
        Closer closer = Closer.create();
        String h = "garbage mongodb://host:port/database";
        try {
            NodeStore store = bootstrapNodeStore(args, closer, h);
            if (!(store instanceof DocumentNodeStore)) {
                System.err.println("Garbage mode only available for DocumentNodeStore");
                System.exit(1);
            }
            DocumentNodeStore dns = (DocumentNodeStore) store;

            DocumentNodeStoreHelper.garbageReport(dns);
        } catch (Throwable e) {
            throw closer.rethrow(e);
        } finally {
            closer.close();
        }
    }

    private static void debug(String[] args) throws IOException {
        if (args.length == 0) {
            System.err.println("usage: debug <path> [id...]");
            System.exit(1);
        } else if (!isValidFileStore(args[0])) {
            System.err.println("Invalid FileStore directory " + args[0]);
            System.exit(1);
        } else {
            // TODO: enable debug information for other node store implementations
            System.out.println("Debug " + args[0]);
            File file = new File(args[0]);
            FileStore store = newFileStore(file)
                .withMaxFileSize(256)
                .withMemoryMapping(false)
                .create();
            try {
                if (args.length == 1) {
                    debugFileStore(store);
                } else {
                    if (args[1].endsWith(".tar")) {
                        debugTarFile(store, args);
                    } else {
                        debugSegment(store, args);
                    }
                }
            } finally {
                store.close();
            }
        }
    }

    private static void graph(String[] args) throws Exception {
        OptionParser parser = new OptionParser();
        OptionSpec<File> directoryArg = parser.nonOptions(
                "Path to segment store (required)").ofType(File.class);
        OptionSpec<File> outFileArg = parser.accepts(
                "output", "Output file").withRequiredArg().ofType(File.class)
                .defaultsTo(new File("segments.gdf"));
        OptionSpec<Long> epochArg = parser.accepts(
                "epoch", "Epoch of the segment time stamps (derived from journal.log if not given)")
                .withRequiredArg().ofType(Long.class);
        OptionSet options = parser.parse(args);

        File directory = directoryArg.value(options);
        if (directory == null) {
            System.err.println("Dump the segment graph to a file. Usage: graph [File] <options>");
            parser.printHelpOn(System.err);
            System.exit(-1);
        }
        if (!isValidFileStore(directory.getPath())) {
            System.err.println("Invalid FileStore directory " + directory);
            System.exit(1);
        }

        File outFile = outFileArg.value(options);
        Date epoch;
        if (options.has(epochArg)) {
            epoch = new Date(epochArg.value(options));
        } else {
            Calendar c = Calendar.getInstance();
            c.setTimeInMillis(new File(directory, "journal.log").lastModified());
            c.set(Calendar.HOUR_OF_DAY, 0);
            c.set(Calendar.MINUTE, 0);
            c.set(Calendar.SECOND, 0);
            c.set(Calendar.MILLISECOND, 0);
            epoch = c.getTime();
        }

        System.out.println("Opening file store at " + directory);
        ReadOnlyStore fileStore = new ReadOnlyStore(directory);

        if (outFile.exists()) {
            outFile.delete();
        }

        System.out.println("Setting epoch to " + epoch);
        System.out.println("Writing graph to " + outFile);
        FileStoreHelper.writeSegmentGraph(fileStore, new FileOutputStream(outFile), epoch);
    }

    private static void check(String[] args) throws IOException {
        OptionParser parser = new OptionParser();
        ArgumentAcceptingOptionSpec<String> path = parser.accepts(
                "path", "path to the segment store (required)")
                .withRequiredArg().ofType(String.class);
        ArgumentAcceptingOptionSpec<String> journal = parser.accepts(
                "journal", "journal file")
                .withRequiredArg().ofType(String.class).defaultsTo("journal.log");
        ArgumentAcceptingOptionSpec<Long> deep = parser.accepts(
                "deep", "enable deep consistency checking. An optional long " +
                        "specifies the number of seconds between progress notifications")
                .withOptionalArg().ofType(Long.class).defaultsTo(Long.MAX_VALUE);
        ArgumentAcceptingOptionSpec<Long> bin = parser.accepts(
                "bin", "read the n first bytes from binary properties. -1 for all bytes.")
                .withOptionalArg().ofType(Long.class).defaultsTo(0L);

        OptionSet options = parser.parse(args);

        if (!options.has(path)) {
            System.err.println("usage: check <options>");
            parser.printHelpOn(System.err);
            System.exit(1);
        }

        if (!isValidFileStore(path.value(options))) {
            System.err.println("Invalid FileStore directory " + args[0]);
            System.exit(1);
        }

        File dir = new File(path.value(options));
        String journalFileName = journal.value(options);
        boolean fullTraversal = options.has(deep);
        long debugLevel = deep.value(options);
        long binLen = bin.value(options);
        checkConsistency(dir, journalFileName, fullTraversal, debugLevel, binLen);
    }

    private static void debugTarFile(FileStore store, String[] args) {
        File root = new File(args[0]);
        for (int i = 1; i < args.length; i++) {
            String f = args[i];
            if (!f.endsWith(".tar")) {
                System.out.println("skipping " + f);
                continue;
            }
            File tar = new File(root, f);
            if (!tar.exists()) {
                System.out.println("file doesn't exist, skipping " + f);
                continue;
            }
            System.out.println("Debug file " + tar + "(" + tar.length() + ")");
            Set<UUID> uuids = new HashSet<UUID>();
            boolean hasrefs = false;
            for (Entry<String, Set<UUID>> e : store.getTarReaderIndex()
                    .entrySet()) {
                if (e.getKey().endsWith(f)) {
                    hasrefs = true;
                    uuids = e.getValue();
                }
            }
            if (hasrefs) {
                System.out.println("SegmentNodeState references to " + f);
                List<String> paths = new ArrayList<String>();
                NodeStoreTree.filterNodeStates(uuids, paths, store.getHead(),
                        "/");
                for (String p : paths) {
                    System.out.println("  " + p);
                }
            } else {
                System.out.println("No references to " + f);
            }

            try {
                Map<UUID, List<UUID>> graph = store.getTarGraph(f);
                System.out.println();
                System.out.println("Tar graph:");
                for (Entry<UUID, List<UUID>> entry : graph.entrySet()) {
                    System.out.println("" + entry.getKey() + '=' + entry.getValue());
                }
            } catch (IOException e) {
                System.out.println("Error getting tar graph:");
            }

        }
    }

    private static void debugSegment(FileStore store, String[] args) {
        Pattern pattern = Pattern
                .compile("([0-9a-f-]+)|(([0-9a-f-]+:[0-9a-f]+)(-([0-9a-f-]+:[0-9a-f]+))?)?(/.*)?");
        for (int i = 1; i < args.length; i++) {
            Matcher matcher = pattern.matcher(args[i]);
            if (!matcher.matches()) {
                System.err.println("Unknown argument: " + args[i]);
            } else if (matcher.group(1) != null) {
                UUID uuid = UUID.fromString(matcher.group(1));
                SegmentId id = store.getTracker().getSegmentId(
                        uuid.getMostSignificantBits(),
                        uuid.getLeastSignificantBits());
                System.out.println(id.getSegment());
            } else {
                RecordId id1 = store.getHead().getRecordId();
                RecordId id2 = null;
                if (matcher.group(2) != null) {
                    id1 = RecordId.fromString(store.getTracker(),
                            matcher.group(3));
                    if (matcher.group(4) != null) {
                        id2 = RecordId.fromString(store.getTracker(),
                                matcher.group(5));
                    }
                }
                String path = "/";
                if (matcher.group(6) != null) {
                    path = matcher.group(6);
                }

                if (id2 == null) {
                    NodeState node = new SegmentNodeState(id1);
                    System.out.println("/ (" + id1 + ") -> " + node);
                    for (String name : PathUtils.elements(path)) {
                        node = node.getChildNode(name);
                        RecordId nid = null;
                        if (node instanceof SegmentNodeState) {
                            nid = ((SegmentNodeState) node).getRecordId();
                        }
                        System.out.println("  " + name + " (" + nid + ") -> "
                                + node);
                    }
                } else {
                    NodeState node1 = new SegmentNodeState(id1);
                    NodeState node2 = new SegmentNodeState(id2);
                    for (String name : PathUtils.elements(path)) {
                        node1 = node1.getChildNode(name);
                        node2 = node2.getChildNode(name);
                    }
                    System.out.println(JsopBuilder.prettyPrint(JsopDiff
                            .diffToJsop(node1, node2)));
                }
            }
        }
    }

    private static void debugFileStore(FileStore store) {
        Map<SegmentId, List<SegmentId>> idmap = Maps.newHashMap();
        int dataCount = 0;
        long dataSize = 0;
        int bulkCount = 0;
        long bulkSize = 0;

        ((Logger) getLogger(SegmentTracker.class)).setLevel(Level.OFF);
        RecordUsageAnalyser analyser = new RecordUsageAnalyser();

        for (SegmentId id : store.getSegmentIds()) {
            if (id.isDataSegmentId()) {
                Segment segment = id.getSegment();
                dataCount++;
                dataSize += segment.size();
                idmap.put(id, segment.getReferencedIds());
                analyseSegment(segment, analyser);
            } else if (id.isBulkSegmentId()) {
                bulkCount++;
                bulkSize += id.getSegment().size();
                idmap.put(id, Collections.<SegmentId>emptyList());
            }
        }
        System.out.println("Total size:");
        System.out.format(
                "%s in %6d data segments%n",
                byteCountToDisplaySize(dataSize), dataCount);
        System.out.format(
                "%s in %6d bulk segments%n",
                byteCountToDisplaySize(bulkSize), bulkCount);
        System.out.println(analyser.toString());

        Set<SegmentId> garbage = newHashSet(idmap.keySet());
        Queue<SegmentId> queue = Queues.newArrayDeque();
        queue.add(store.getHead().getRecordId().getSegmentId());
        while (!queue.isEmpty()) {
            SegmentId id = queue.remove();
            if (garbage.remove(id)) {
                queue.addAll(idmap.get(id));
            }
        }
        dataCount = 0;
        dataSize = 0;
        bulkCount = 0;
        bulkSize = 0;
        for (SegmentId id : garbage) {
            if (id.isDataSegmentId()) {
                dataCount++;
                dataSize += id.getSegment().size();
            } else if (id.isBulkSegmentId()) {
                bulkCount++;
                bulkSize += id.getSegment().size();
            }
        }
        System.out.format("%nAvailable for garbage collection:%n");
        System.out.format("%s in %6d data segments%n",
                byteCountToDisplaySize(dataSize), dataCount);
        System.out.format("%s in %6d bulk segments%n",
                byteCountToDisplaySize(bulkSize), bulkCount);
        System.out.format("%n%s", new PCMAnalyser(store).toString());
    }

    private static void analyseSegment(Segment segment, RecordUsageAnalyser analyser) {
        for (int k = 0; k < segment.getRootCount(); k++) {
            if (segment.getRootType(k) == NODE) {
                RecordId nodeId = new RecordId(segment.getSegmentId(), segment.getRootOffset(k));
                try {
                    analyser.analyseNode(nodeId);
                } catch (Exception e) {
                    System.err.format("Error while processing node at %s", nodeId);
                    e.printStackTrace();
                }
            }
        }
    }

    /**
     * Checks if the provided directory is a valid FileStore
     * 
     * @return true if the provided directory is a valid FileStore
     */
    private static boolean isValidFileStore(String path) {
        File store = new File(path);
        if (!store.isDirectory()) {
            return false;
        }
        // for now the only check is the existence of the journal file
        for (String f : store.list()) {
            if ("journal.log".equals(f)) {
                return true;
            }
        }
        return false;
    }

    private static void server(String defaultUri, String[] args) throws Exception {
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
        String uri = (arglist.isEmpty()) ? defaultUri : arglist.get(0);
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

        } else if (fix.equals(OakFixture.OAK_TAR)) {
            File baseFile = base.value(options);
            if (baseFile == null) {
                throw new IllegalArgumentException("Required argument base missing.");
            }
            oakFixture = OakFixture.getTar(OakFixture.OAK_TAR, baseFile, 256, cacheSize, mmap.value(options), false);
        } else if (fix.equals(OakFixture.OAK_RDB)) {
            oakFixture = OakFixture.getRDB(OakFixture.OAK_RDB, rdbjdbcuri.value(options), rdbjdbcuser.value(options),
                    rdbjdbcpasswd.value(options), rdbjdbctableprefix.value(options), false, cacheSize);
        } else {
            throw new IllegalArgumentException("Unsupported repository setup " + fix);
        }

        startOakServer(oakFixture, uri, cIds);
    }

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
        new HttpServer(uri, m);
    }

    public static class HttpServer {

        private final ServletContextHandler context;

        private final Server server;

        public HttpServer(String uri) throws Exception {
            this(uri, Collections.singletonMap(new Oak(), ""));
        }

        public HttpServer(String uri, Map<Oak, String> oakMap) throws Exception {
            int port = java.net.URI.create(uri).getPort();
            if (port == -1) {
                // use default
                port = PORT;
            }

            context = new ServletContextHandler();
            context.setContextPath("/");

            for (Map.Entry<Oak, String> entry : oakMap.entrySet()) {
                addServlets(entry.getKey(), entry.getValue());
            }

            server = new Server(port);
            server.setHandler(context);
            server.start();
        }

        public void join() throws Exception {
            server.join();
        }

        public void stop() throws Exception {
            server.stop();
        }

        private void addServlets(Oak oak, String path) {
            Jcr jcr = new Jcr(oak);

            // 1 - OakServer
            ContentRepository repository = jcr.createContentRepository();
            ServletHolder holder = new ServletHolder(new OakServlet(repository));
            context.addServlet(holder, path + "/*");

            ServletHolder remoteServlet = new ServletHolder(new RemoteServlet(new ContentRemoteRepository(repository)));
            context.addServlet(remoteServlet, path + "/api/*");

            // 2 - Webdav Server on JCR repository
            final Repository jcrRepository = jcr.createRepository();
            @SuppressWarnings("serial")
            ServletHolder webdav = new ServletHolder(new SimpleWebdavServlet() {
                @Override
                public Repository getRepository() {
                    return jcrRepository;
                }
            });
            webdav.setInitParameter(SimpleWebdavServlet.INIT_PARAM_RESOURCE_PATH_PREFIX, path + "/webdav");
            webdav.setInitParameter(AbstractWebdavServlet.INIT_PARAM_AUTHENTICATE_HEADER, "Basic realm=\"Oak\"");
            context.addServlet(webdav, path + "/webdav/*");

            // 3 - JCR Remoting Server
            @SuppressWarnings("serial")
            ServletHolder jcrremote = new ServletHolder(new JcrRemotingServlet() {
                @Override
                protected Repository getRepository() {
                    return jcrRepository;
                }
            });
            jcrremote.setInitParameter(JCRWebdavServerServlet.INIT_PARAM_RESOURCE_PATH_PREFIX, path + "/jcrremote");
            jcrremote.setInitParameter(AbstractWebdavServlet.INIT_PARAM_AUTHENTICATE_HEADER, "Basic realm=\"Oak\"");
            context.addServlet(jcrremote, path + "/jcrremote/*");
        }

    }

    public enum Mode {

        BACKUP("backup"),
        RESTORE("restore"),
        BENCHMARK("benchmark"),
        CONSOLE("console"),
        DEBUG("debug"),
        GRAPH("graph"),
        CHECK("check"),
        COMPACT("compact"),
        SERVER("server"),
        UPGRADE("upgrade"),
        SCALABILITY("scalability"),
        EXPLORE("explore"),
        PRIMARY("primary"),
        STANDBY("standy"),
        HELP("help"),
        CHECKPOINTS("checkpoints"),
        RECOVERY("recovery"),
        REPAIR("repair"),
        TIKA("tika"),
        GARBAGE("garbage"),
        TARMKDIFF("tarmkdiff");

        private final String name;

        Mode(String name) {
            this.name = name;
        }

        @Override
        public String toString() {
            return name;
        }
    }
}
