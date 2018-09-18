/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.jackrabbit.oak.plugins.document.mongo;

import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.common.base.Joiner;
import com.mongodb.MongoClient;

import org.bson.Document;
import org.junit.rules.ExternalResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.flapdoodle.embed.mongo.Command;
import de.flapdoodle.embed.mongo.MongodStarter;
import de.flapdoodle.embed.mongo.config.DownloadConfigBuilder;
import de.flapdoodle.embed.mongo.config.ExtractedArtifactStoreBuilder;
import de.flapdoodle.embed.mongo.config.RuntimeConfigBuilder;
import de.flapdoodle.embed.process.config.IRuntimeConfig;
import de.flapdoodle.embed.process.io.directories.FixedPath;
import de.flapdoodle.embed.process.io.directories.IDirectory;
import de.flapdoodle.embed.process.runtime.Network;

import static de.flapdoodle.embed.process.io.directories.Directories.join;
import static org.junit.Assert.assertTrue;

/**
 * External resource for mongod processes.
 */
public class MongodProcessFactory extends ExternalResource {

    private static final Logger LOG = LoggerFactory.getLogger(MongodProcessFactory.class);

    private static final IDirectory EXTRACT_DIR = join(new FixedPath("target"), new FixedPath("mongo-extracted"));

    private static final IDirectory DOWNLOAD_DIR = join(new FixedPath("target"), new FixedPath("mongo-download"));

    private static final IDirectory TMP_DIR = join(new FixedPath("target"), new FixedPath("tmp"));

    static {
        System.setProperty("de.flapdoodle.embed.io.tmpdir", TMP_DIR.asFile().getAbsolutePath());
    }

    private static final IRuntimeConfig CONFIG = new RuntimeConfigBuilder()
            .defaultsWithLogger(Command.MongoD, LoggerFactory.getLogger(MongodProcessFactory.class))
            .artifactStore(new ExtractedArtifactStoreBuilder()
                    .defaults(Command.MongoD)
                    .download(new DownloadConfigBuilder()
                            .defaultsForCommand(Command.MongoD)
                            .artifactStorePath(DOWNLOAD_DIR).build())
                    .extractDir(EXTRACT_DIR).build())
            .daemonProcess(false)
            .build();

    private static final MongodStarter STARTER = MongodStarter.getInstance(CONFIG);

    private final Map<Integer, MongodProcess> processes = new HashMap<>();

    public Map<Integer, MongodProcess> startReplicaSet(String replicaSetName, int size)
            throws IOException {
        int[] ports = Network.getFreeServerPorts(InetAddress.getLoopbackAddress(), size);
        return startReplicaSet(replicaSetName, ports);
    }

    public Map<Integer, MongodProcess> startReplicaSet(
            String replicaSetName, int[] ports) throws IOException {
        assertTrue(ports.length > 0);
        Map<Integer, MongodProcess> executables = new HashMap<>();
        for (int p : ports) {
            MongodProcess proc = new MongodProcess(STARTER, replicaSetName, p);
            proc.start();
            processes.put(p, proc);
            executables.put(p, proc);
        }
        initRS(replicaSetName, ports);
        return executables;
    }

    @Override
    protected void before() {
        if (!EXTRACT_DIR.asFile().exists()) {
            assertTrue(EXTRACT_DIR.asFile().mkdirs());
        }
        if (!TMP_DIR.asFile().exists()) {
            assertTrue(TMP_DIR.asFile().mkdirs());
        }
        if (!DOWNLOAD_DIR.asFile().exists()) {
            assertTrue(DOWNLOAD_DIR.asFile().mkdirs());
        }
    }

    @Override
    protected void after() {
        processes.forEach((port, process) -> {
            if (process.isStopped()) {
                LOG.info("MongoDB on port {} already stopped", port);
            }
            LOG.info("Stopping MongoDB on port {}", port);
            try {
                process.stop();
            } catch (Exception e) {
                LOG.error("Exception stopping MongoDB process", e);
            }
        });
        processes.clear();
    }

    static String localhost(Integer... ports) {
        return localhost(Arrays.asList(ports));
    }

    static String localhost(Iterable<Integer> ports) {
        String portsString = Joiner.on(',').join(ports);
        String host = InetAddress.getLoopbackAddress().getHostAddress();
        if (!portsString.isEmpty()) {
            host += ":" + portsString;
        }
        return host;
    }

    //----------------------------< internal >----------------------------------

    private void initRS(String rs, int[] ports) {
        List<Document> members = new ArrayList<>();
        for (int i = 0; i < ports.length; i++) {
            members.add(new Document("_id", i).append("host", localhost(ports[i])));
        }
        Document config = new Document("_id", rs);
        config.append("members", members);
        try (MongoClient c = new MongoClient(localhost(), ports[0])) {
            c.getDatabase("admin").runCommand(
                    new Document("replSetInitiate", config));
        }
    }
}
