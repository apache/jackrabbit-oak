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

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Queue;
import java.util.Set;
import java.util.UUID;
import javax.jcr.Repository;

import com.google.common.collect.Maps;
import com.google.common.collect.Queues;
import org.apache.jackrabbit.core.RepositoryContext;
import org.apache.jackrabbit.core.config.RepositoryConfig;
import org.apache.jackrabbit.mk.core.MicroKernelImpl;
import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.api.ContentRepository;
import org.apache.jackrabbit.oak.benchmark.BenchmarkRunner;
import org.apache.jackrabbit.oak.http.OakServlet;
import org.apache.jackrabbit.oak.jcr.Jcr;
import org.apache.jackrabbit.oak.kernel.KernelNodeStore;
import org.apache.jackrabbit.oak.plugins.backup.FileStoreBackup;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.apache.jackrabbit.oak.plugins.segment.Segment;
import org.apache.jackrabbit.oak.plugins.segment.SegmentIdFactory;
import org.apache.jackrabbit.oak.plugins.segment.SegmentNodeStore;
import org.apache.jackrabbit.oak.plugins.segment.file.FileStore;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.upgrade.RepositoryUpgrade;
import org.apache.jackrabbit.server.remoting.davex.JcrRemotingServlet;
import org.apache.jackrabbit.webdav.jcr.JCRWebdavServerServlet;
import org.apache.jackrabbit.webdav.server.AbstractWebdavServlet;
import org.apache.jackrabbit.webdav.simple.SimpleWebdavServlet;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;

import static com.google.common.collect.Sets.newHashSet;

public class Main {

    public static final int PORT = 8080;
    public static final String URI = "http://localhost:" + PORT + "/";

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
            case BENCHMARK:
                BenchmarkRunner.main(args);
                break;
            case DEBUG:
                debug(args);
                break;
            case SERVER:
                server(URI, args);
                break;
            case UPGRADE:
                upgrade(args);
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
            FileStore store = new FileStore(new File(args[0]), 256, false);
            FileStoreBackup.backup(new SegmentNodeStore(store), new File(args[1]));
            store.close();
        } else {
            System.err.println("usage: backup <repository> <backup>");
            System.exit(1);
        }
    }

    private static void debug(String[] args) throws IOException {
        if (args.length == 0) {
            System.err.println("usage: debug <path> [id...]");
            System.exit(1);
        } else {
            // TODO: enable debug information for other node store implementations
            System.out.println("Debug " + args[0]);
            File file = new File(args[0]);
            FileStore store = new FileStore(file, 256, false);
            try {
                if (args.length == 1) {
                    Map<UUID, List<UUID>> idmap = Maps.newHashMap();

                    int dataCount = 0;
                    long dataSize = 0;
                    int bulkCount = 0;
                    long bulkSize = 0;
                    for (UUID uuid : store.getSegmentIds()) {
                        if (SegmentIdFactory.isDataSegmentId(uuid)) {
                            Segment segment = store.readSegment(uuid);
                            dataCount++;
                            dataSize += segment.size();
                            idmap.put(uuid, segment.getReferencedIds());
                        } else if (SegmentIdFactory.isBulkSegmentId(uuid)) {
                            bulkCount++;
                            bulkSize += store.readSegment(uuid).size();
                            idmap.put(uuid, Collections.<UUID>emptyList());
                        }
                    }
                    System.out.println("Total size:");
                    System.out.format(
                            "%6dMB in %6d data segments%n",
                            dataSize / (1024 * 1024), dataCount);
                    System.out.format(
                            "%6dMB in %6d bulk segments%n",
                            bulkSize / (1024 * 1024), bulkCount);

                    Set<UUID> garbage = newHashSet(idmap.keySet());
                    Queue<UUID> queue = Queues.newArrayDeque();
                    queue.add(store.getHead().getRecordId().getSegmentId());
                    while (!queue.isEmpty()) {
                        UUID id = queue.remove();
                        if (garbage.remove(id)) {
                            queue.addAll(idmap.get(id));
                        }
                    }
                    dataCount = 0;
                    dataSize = 0;
                    bulkCount = 0;
                    bulkSize = 0;
                    for (UUID uuid : garbage) {
                        if (SegmentIdFactory.isDataSegmentId(uuid)) {
                            dataCount++;
                            dataSize += store.readSegment(uuid).size();
                        } else if (SegmentIdFactory.isBulkSegmentId(uuid)) {
                            bulkCount++;
                            bulkSize += store.readSegment(uuid).size();
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
                    for (int i = 1; i < args.length; i++) {
                        UUID uuid = UUID.fromString(args[i]);
                        System.out.println(store.readSegment(uuid));
                    }
                }
            } finally {
                store.close();
            }
        }
    }

    private static void upgrade(String[] args) throws Exception {
        if (args.length == 2) {
            RepositoryContext source = RepositoryContext.create(RepositoryConfig.create(new File(args[0])));
            try {
                FileStore store = new FileStore(new File(args[1]), 256, true);
                try {
                    NodeStore target = new SegmentNodeStore(store);
                    new RepositoryUpgrade(source, target).copy();
                } finally {
                    store.close();
                }
            } finally {
                source.getRepository().shutdown();
            }
        } else {
            System.err.println("usage: upgrade <olddir> <newdir>");
            System.exit(1);
        }
    }

    private static void server(String uri, String[] args) throws Exception {
        // TODO add support for different repo implementations (see fixtures for benchmarks)
        Map<NodeStore, String> storeMap;
        if (args.length == 0) {
            System.out.println("Starting an in-memory repository");
            System.out.println(uri + " -> [memory]");
            NodeStore store = new MemoryNodeStore();
            storeMap = Collections.singletonMap(store, "");
        } else if (args.length == 1) {
            System.out.println("Starting a standalone repository");
            System.out.println(uri + " -> " + args[0]);
            NodeStore store = new KernelNodeStore(new MicroKernelImpl(args[0]));
            storeMap = Collections.singletonMap(store, "");
        } else {
            System.out.println("Starting a clustered repository");
            storeMap = new HashMap<NodeStore, String>(args.length);
            for (int i = 0; i < args.length; i++) {
                // FIXME: Use a clustered MicroKernel implementation
                System.out.println(uri + "/node" + i + "/ -> " + args[i]);
                KernelNodeStore store = new KernelNodeStore(new MicroKernelImpl(args[i]));
                storeMap.put(store, "/node" + i);
            }
        }
        new HttpServer(uri, storeMap);
    }

    public static class HttpServer {

        private final ServletContextHandler context;

        private final Server server;

        public HttpServer(String uri) throws Exception {
            this(uri, Collections.singletonMap(new MemoryNodeStore(), ""));
        }

        public HttpServer(String uri, Map<? extends NodeStore, String> storeMap) throws Exception {
            int port = java.net.URI.create(uri).getPort();
            if (port == -1) {
                // use default
                port = PORT;
            }

            context = new ServletContextHandler();
            context.setContextPath("/");

            for (Map.Entry<? extends NodeStore, String> entry : storeMap.entrySet()) {
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

        private void addServlets(NodeStore store, String path) {
            Oak oak = new Oak(store);
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
        BENCHMARK("benchmark"),
        DEBUG("debug"),
        SERVER("server"),
        UPGRADE("upgrade");

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
