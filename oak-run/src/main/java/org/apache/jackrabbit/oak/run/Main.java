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
import static org.apache.jackrabbit.oak.checkpoint.Checkpoints.CP;

import java.io.BufferedWriter;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Queue;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.jcr.Repository;

import com.google.common.base.Charsets;
import com.google.common.base.Joiner;
import com.google.common.base.StandardSystemProperty;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.io.Closer;
import com.google.common.io.Files;
import com.mongodb.MongoURI;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;

import org.apache.jackrabbit.core.RepositoryContext;
import org.apache.jackrabbit.core.config.RepositoryConfig;
import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.ContentRepository;
import org.apache.jackrabbit.oak.benchmark.BenchmarkRunner;
import org.apache.jackrabbit.oak.checkpoint.Checkpoints;
import org.apache.jackrabbit.oak.commons.IOUtils;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.fixture.OakFixture;
import org.apache.jackrabbit.oak.http.OakServlet;
import org.apache.jackrabbit.oak.jcr.Jcr;
import org.apache.jackrabbit.oak.plugins.backup.FileStoreBackup;
import org.apache.jackrabbit.oak.plugins.backup.FileStoreRestore;
import org.apache.jackrabbit.oak.plugins.blob.BlobReferenceRetriever;
import org.apache.jackrabbit.oak.plugins.blob.ReferenceCollector;
import org.apache.jackrabbit.oak.plugins.document.DocumentBlobReferenceRetriever;
import org.apache.jackrabbit.oak.plugins.document.DocumentMK;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeStore;
import org.apache.jackrabbit.oak.plugins.document.mongo.MongoDocumentStore;
import org.apache.jackrabbit.oak.plugins.document.mongo.MongoDocumentStoreHelper;
import org.apache.jackrabbit.oak.plugins.segment.RecordId;
import org.apache.jackrabbit.oak.plugins.segment.Segment;
import org.apache.jackrabbit.oak.plugins.segment.SegmentBlobReferenceRetriever;
import org.apache.jackrabbit.oak.plugins.segment.SegmentId;
import org.apache.jackrabbit.oak.plugins.segment.SegmentNodeState;
import org.apache.jackrabbit.oak.plugins.segment.SegmentNodeStore;
import org.apache.jackrabbit.oak.plugins.segment.compaction.CompactionStrategy;
import org.apache.jackrabbit.oak.plugins.segment.compaction.CompactionStrategy.CleanupType;
import org.apache.jackrabbit.oak.plugins.segment.file.FileStore;
import org.apache.jackrabbit.oak.plugins.segment.standby.client.StandbyClient;
import org.apache.jackrabbit.oak.plugins.segment.standby.server.StandbyServer;
import org.apache.jackrabbit.oak.plugins.tika.TextExtractorMain;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.apache.jackrabbit.oak.spi.blob.GarbageCollectableBlobStore;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.upgrade.RepositoryUpgrade;
import org.apache.jackrabbit.server.remoting.davex.JcrRemotingServlet;
import org.apache.jackrabbit.webdav.jcr.JCRWebdavServerServlet;
import org.apache.jackrabbit.webdav.server.AbstractWebdavServlet;
import org.apache.jackrabbit.webdav.simple.SimpleWebdavServlet;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;

import com.google.common.collect.Maps;
import com.google.common.collect.Queues;
import com.google.common.util.concurrent.AbstractScheduledService;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;

public class Main {

    public static final int PORT = 8080;
    public static final String URI = "http://localhost:" + PORT + "/";

    private static final int MB = 1024 * 1024;

    public static final boolean TAR_STORAGE_MEMORY_MAPPED = Boolean.getBoolean("tar.memoryMapped");

    private Main() {
    }

    public static void main(String[] args) throws Exception {
        printProductInfo();

        Mode mode = Mode.SERVER;
        if (args.length > 0) {
            mode = Mode.valueOf(args[0].toUpperCase());
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
            case DEBUG:
                debug(args);
                break;
            case COMPACT:
                compact(args);
                break;
            case SERVER:
                server(URI, args);
                break;
            case UPGRADE:
                upgrade(args);
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
            case REPAIR:
                repair(args);
                break;
            case TIKA:
                TextExtractorMain.main(args);
                break;
            case DUMPDATASTOREREFS:
                dumpBlobRefs(args);
                break;            
            default:
                System.err.println("Unknown command: " + mode);
                System.exit(1);

        }
    }

    private static void printProductInfo() {
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

        System.out.println(product);
    }

    private static void backup(String[] args) throws IOException {
        if (args.length == 2) {
            // TODO: enable backup for other node store implementations
            FileStore store = new FileStore(new File(args[0]), 256, TAR_STORAGE_MEMORY_MAPPED);
            FileStoreBackup.backup(new SegmentNodeStore(store), new File(args[1]));
            store.close();
        } else {
            System.err.println("usage: backup <repository> <backup>");
            System.exit(1);
        }
    }

    private static void restore(String[] args) throws IOException {
        if (args.length == 2) {
            // TODO: enable restore for other node store implementations
            FileStore store = new FileStore(new File(args[0]), 256, false);
            File target = new File(args[1]);
            try {
                FileStoreRestore.restore(target, new SegmentNodeStore(store));
            } catch (CommitFailedException e) {
                throw new IOException(e);
            }
            store.close();
        } else {
            System.err.println("usage: restore <repository> <backup>");
            System.exit(1);
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

    private static void compact(String[] args) throws IOException {
        if (args.length != 1) {
            System.err.println("usage: compact <path>");
            System.exit(1);
        } else if (!isValidFileStore(args[0])) {
            System.err.println("Invalid FileStore directory " + args[0]);
            System.exit(1);
        } else {
            File directory = new File(args[0]);
            System.out.println("Compacting " + directory);
            System.out.println("    before " + Arrays.toString(directory.list()));

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
                store.setCompactionStrategy(compactionStrategy);
                store.compact();
            } finally {
                store.close();
            }

            System.out.println("    -> cleaning up");
            store = openFileStore(directory);
            try {
                store.cleanup();
            } finally {
                store.close();
            }

            System.out.println("    after  " + Arrays.toString(directory.list()));
        }
    }

    private static FileStore openFileStore(File directory) throws IOException {
        return FileStore
                .newFileStore(directory)
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

    private static void repair(String[] args) throws IOException {
        if (args.length < 2) {
            System.err.println("usage: repair mongodb://host:port/database path");
            return;
        }
        Closer closer = Closer.create();
        try {
            final DocumentNodeStore store;
            if (args[0].startsWith(MongoURI.MONGODB_PREFIX)) {
                MongoClientURI uri = new MongoClientURI(args[0]);
                MongoClient client = new MongoClient(uri);
                store = new DocumentMK.Builder().setMongoDB(client.getDB(uri.getDatabase())).getNodeStore();
                closer.register(new Closeable() {
                    @Override
                    public void close() throws IOException {
                        store.dispose();
                    }
                });
            } else {
                System.err.println("Repair only available for MongoDocumentStore");
                System.exit(1);
                return;
            }

            MongoDocumentStore docStore = (MongoDocumentStore) store.getDocumentStore();

            String path = args[args.length - 1];
            MongoDocumentStoreHelper.repair(docStore, path);
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
            FileStore store = new FileStore(file, 256, TAR_STORAGE_MEMORY_MAPPED);
            try {
                if (args.length == 1) {
                    Map<SegmentId, List<SegmentId>> idmap = Maps.newHashMap();

                    int dataCount = 0;
                    long dataSize = 0;
                    int bulkCount = 0;
                    long bulkSize = 0;
                    for (SegmentId id : store.getSegmentIds()) {
                        if (id.isDataSegmentId()) {
                            Segment segment = id.getSegment();
                            dataCount++;
                            dataSize += segment.size();
                            idmap.put(id, segment.getReferencedIds());
                            System.out.println(id + " -> " + idmap.get(id));
                        } else if (id.isBulkSegmentId()) {
                            bulkCount++;
                            bulkSize += id.getSegment().size();
                            idmap.put(id, Collections.<SegmentId>emptyList());
                        }
                    }
                    System.out.println("Total size:");
                    System.out.format(
                            "%6dMB in %6d data segments%n",
                            dataSize / (1024 * 1024), dataCount);
                    System.out.format(
                            "%6dMB in %6d bulk segments%n",
                            bulkSize / (1024 * 1024), bulkCount);

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
                    System.out.println("Available for garbage collection:");
                    System.out.format(
                            "%6dMB in %6d data segments%n",
                            dataSize / (1024 * 1024), dataCount);
                    System.out.format(
                            "%6dMB in %6d bulk segments%n",
                            bulkSize / (1024 * 1024), bulkCount);
                } else {
                    Pattern pattern = Pattern.compile(
                            "([0-9a-f-]+)|([0-9a-f-]+:[0-9a-f]+)?(/.*)?");
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
                            RecordId id = store.getHead().getRecordId();
                            if (matcher.group(2) != null) {
                                id = RecordId.fromString(
                                        store.getTracker(), matcher.group(2));
                            }
                            String path = "/";
                            if (matcher.group(3) != null) {
                                path = matcher.group(3);
                            }
                            NodeState node = new SegmentNodeState(id);
                            System.out.println("/ -> " + node);
                            for (String name : PathUtils.elements(path)) {
                                node = node.getChildNode(name);
                                System.out.println(" " + name  + " -> " + node);
                            }
                        }
                    }
                }
            } finally {
                store.close();
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

    private static void upgrade(String[] args) throws Exception {
        OptionParser parser = new OptionParser();
        parser.accepts("datastore", "keep data store");
        OptionSpec<String> nonOption = parser.nonOptions();
        OptionSet options = parser.parse(args);

        List<String> argList = nonOption.values(options);
        if (argList.size() == 2 || argList.size() == 3) {
            File dir = new File(argList.get(0));
            File xml = new File(dir, "repository.xml");
            String dst = argList.get(1);
            if (argList.size() == 3) {
                xml = new File(dst);
                dst = argList.get(2);
            }

            RepositoryContext source =
                    RepositoryContext.create(RepositoryConfig.create(dir, xml));
            try {
                if (dst.startsWith("mongodb://")) {
                    MongoClientURI uri = new MongoClientURI(dst);
                    MongoClient client = new MongoClient(uri);
                    try {
                        DocumentNodeStore target = new DocumentMK.Builder()
                            .setMongoDB(client.getDB(uri.getDatabase()))
                            .getNodeStore();
                        try {
                            RepositoryUpgrade upgrade =
                                    new RepositoryUpgrade(source, target);
                            upgrade.setCopyBinariesByReference(
                                    options.has("datastore"));
                            upgrade.copy(null);
                        } finally {
                            target.dispose();
                        }
                    } finally {
                        client.close();
                    }
                } else {
                    FileStore store = new FileStore(new File(dst), 256);
                    try {
                        NodeStore target = new SegmentNodeStore(store);
                        RepositoryUpgrade upgrade =
                                new RepositoryUpgrade(source, target);
                        upgrade.setCopyBinariesByReference(
                                options.has("datastore"));
                        upgrade.copy(null);
                    } finally {
                        store.close();
                    }
                }
            } finally {
                source.getRepository().shutdown();
            }
        } else {
            System.err.println("usage: upgrade <olddir> <newdir>");
            System.exit(1);
        }
    }
    
    private static void dumpBlobRefs(String[] args) throws IOException {
        if (args.length == 0) {
            System.out
                .println("usage: dumpdatastorerefs {<path>|<mongo-uri>} <dump_path>]");
            System.exit(1);
        }
        
        Closer closer = Closer.create();
        try {
            BlobReferenceRetriever marker = null;
            BlobStore blobStore = null;
            
            if (args[0].startsWith(MongoURI.MONGODB_PREFIX)) {
                MongoClientURI uri = new MongoClientURI(args[0]);
                MongoClient client = new MongoClient(uri);
                final DocumentNodeStore store = new DocumentMK.Builder().setMongoDB(client.getDB(uri.getDatabase())).getNodeStore();
                blobStore = store.getBlobStore();
                closer.register(new Closeable() {
                    @Override public void close() throws IOException {
                        store.dispose();
                    }
                });
                marker = new DocumentBlobReferenceRetriever(store);
            } else if (isValidFileStore(args[0])) {
                final FileStore store = new FileStore(new File(args[0]), 256, TAR_STORAGE_MEMORY_MAPPED);
                closer.register(new Closeable() {
                    @Override public void close() throws IOException {
                        store.close();
                    }
                });
                marker = new SegmentBlobReferenceRetriever(store.getTracker());
            } else {
                failWith("Invalid FileStore directory " + args[0]);
                return;
            }
            
            String dumpPath = StandardSystemProperty.JAVA_IO_TMPDIR.value();
            if (args.length == 2) {
                dumpPath = args[1];
            }
            File dumpFile = new File(dumpPath, "marked-" + System.currentTimeMillis());
            final BufferedWriter writer = Files.newWriter(dumpFile, Charsets.UTF_8);
            final AtomicInteger count = new AtomicInteger();
            try {
                final List<String> idBatch = Lists.newArrayListWithCapacity(1024);
                final GarbageCollectableBlobStore gcBlobStore =
                    (blobStore != null && blobStore instanceof GarbageCollectableBlobStore
                        ? (GarbageCollectableBlobStore) blobStore
                        : null);
                marker.collectReferences(
                    new ReferenceCollector() {
                        @Override
                        public void addReference(String blobId) {
                            try {
                                Iterator<String> idIter = null;
                                if (gcBlobStore != null) {
                                    idIter = gcBlobStore.resolveChunks(blobId);
                                } else{
                                    idIter = Iterators.singletonIterator(blobId);
                                }
                                
                                while (idIter.hasNext()) {
                                    String id = idIter.next();
                                    idBatch.add(id);
                                    count.getAndIncrement();
                                    if (idBatch.size() >= 1024) {
                                        writer.append(Joiner.on(StandardSystemProperty.LINE_SEPARATOR.value()).join(idBatch));
                                        writer.append(StandardSystemProperty.LINE_SEPARATOR.value());
                                        writer.flush();
                                        idBatch.clear();
                                    }
                                }
                            } catch (Exception e) {
                                throw new RuntimeException("Error in retrieving references", e);
                            }
                        }
                    }
                );
                if (!idBatch.isEmpty()) {
                    writer.append(Joiner.on(StandardSystemProperty.LINE_SEPARATOR.value()).join(idBatch));
                    writer.append(StandardSystemProperty.LINE_SEPARATOR.value());
                    writer.flush();
                    idBatch.clear();
                }
                System.out.println(count.get() + " DataStore references dumped in " + dumpFile);
            } finally {
                IOUtils.closeQuietly(writer);
            }
        } catch (Throwable t) {
            System.err.println(t.getMessage());
        } finally {
            closer.close();
        }
    }
    
    private static void server(String defaultUri, String[] args) throws Exception {
        OptionParser parser = new OptionParser();

        OptionSpec<Integer> cache = parser.accepts("cache", "cache size (MB)").withRequiredArg().ofType(Integer.class).defaultsTo(100);

        // tar/h2 specific option
        OptionSpec<File> base = parser.accepts("base", "Base directory").withRequiredArg().ofType(File.class);
        OptionSpec<Boolean> mmap = parser.accepts("mmap", "TarMK memory mapping").withOptionalArg().ofType(Boolean.class).defaultsTo("64".equals(System.getProperty("sun.arch.data.model")));

        // mongo specific options:
        OptionSpec<String> host = parser.accepts("host", "MongoDB host").withRequiredArg().defaultsTo("localhost");
        OptionSpec<Integer> port = parser.accepts("port", "MongoDB port").withRequiredArg().ofType(Integer.class).defaultsTo(27017);
        OptionSpec<String> dbName = parser.accepts("db", "MongoDB database").withRequiredArg();
        OptionSpec<Integer> clusterIds = parser.accepts("clusterIds", "Cluster Ids").withOptionalArg().ofType(Integer.class).withValuesSeparatedBy(',');
        OptionSpec<String> nonOption = parser.nonOptions();
        OptionSet options = parser.parse(args);

        OakFixture oakFixture;

        List<String> arglist = nonOption.values(options);
        String uri = (arglist.isEmpty()) ? defaultUri : arglist.get(0);
        String fix = (arglist.size() <= 1) ? OakFixture.OAK_MEMORY : arglist.get(1);

        int cacheSize = cache.value(options);
        List<Integer> cIds = Collections.emptyList();
        if (fix.startsWith(OakFixture.OAK_MEMORY)) {
            if (OakFixture.OAK_MEMORY_NS.equals(fix)) {
                oakFixture = OakFixture.getMemoryNS(cacheSize * MB);
            } else if (OakFixture.OAK_MEMORY_MK.equals(fix)) {
                oakFixture = OakFixture.getMemoryMK(cacheSize * MB);
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
            } else if (OakFixture.OAK_MONGO_MK.equals(fix)) {
                oakFixture = OakFixture.getMongoMK(
                        host.value(options), port.value(options),
                        db, false, cacheSize * MB);
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
            oakFixture = OakFixture.getTar(baseFile, 256, cacheSize, mmap.value(options));
        } else if (fix.equals(OakFixture.OAK_H2)) {
            File baseFile = base.value(options);
            if (baseFile == null) {
                throw new IllegalArgumentException("Required argument base missing.");
            }
            oakFixture = OakFixture.getH2MK(baseFile, cacheSize * MB);
        } else {
            throw new IllegalArgumentException("Unsupported repository setup " + fix);
        }

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
            ContentRepository repository = oak.createContentRepository();
            ServletHolder holder = new ServletHolder(new OakServlet(repository));
            context.addServlet(holder, path + "/*");

            // 2 - Webdav Server on JCR repository
            final Repository jcrRepository = jcr.createRepository();
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
        DEBUG("debug"),
        COMPACT("compact"),
        SERVER("server"),
        UPGRADE("upgrade"),
        PRIMARY("primary"),
        STANDBY("standy"),
        CHECKPOINTS("checkpoints"),
        REPAIR("repair"),
        TIKA("tika"),
        DUMPDATASTOREREFS("dumpdatastorerefs");

        private final String name;

        private Mode(String name) {
            this.name = name;
        }

        @Override
        public String toString() {
            return name;
        }
    }
}
