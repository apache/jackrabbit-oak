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

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;

import com.mongodb.ServerAddress;

import org.apache.commons.io.FileUtils;

import de.flapdoodle.embed.mongo.MongodStarter;
import de.flapdoodle.embed.mongo.config.ImmutableMongoCmdOptions;
import de.flapdoodle.embed.mongo.config.ImmutableMongodConfig;
import de.flapdoodle.embed.mongo.config.MongodConfig;
import de.flapdoodle.embed.mongo.config.Net;
import de.flapdoodle.embed.mongo.config.Storage;
import de.flapdoodle.embed.mongo.distribution.Feature;
import de.flapdoodle.embed.mongo.distribution.Versions;
import de.flapdoodle.embed.process.io.directories.Directory;
import de.flapdoodle.embed.process.io.directories.FixedPath;
import de.flapdoodle.embed.process.io.file.Files;
import de.flapdoodle.embed.process.runtime.IStopable;

/**
 * Helper class for starting/stopping a mongod process.
 */
public class MongodProcess {

    private static final String VERSION = "4.2.16";

    private static final Directory TMP_DIR = join(new FixedPath("target"), new FixedPath("tmp"));

    private IStopable process;

    private final MongodStarter starter;

    private final MongodConfig config;

    MongodProcess(MongodStarter starter, String rsName, int port)
            throws IOException {
        this.starter = starter;
        this.config = createConfiguration(rsName, port);
    }

    public synchronized void start() throws IOException {
        if (process != null) {
            throw new IllegalStateException("Already started");
        }
        process = starter.prepare(config).start();
    }

    public synchronized void stop() {
        if (process == null) {
            throw new IllegalStateException("Already stopped");
        }
        process.stop();
        process = null;
    }

    public synchronized boolean isStopped() {
        return process == null;
    }

    public ServerAddress getAddress() {
        return new ServerAddress(config.net().getBindIp(), config.net().getPort());
    }

    static Directory join(final Directory left, final Directory right) {
        return new Directory() {

            @Override
            public boolean isGenerated() {
                return left.isGenerated() || right.isGenerated();
            }

            @Override
            public File asFile() {
                return Files.fileOf(left.asFile(), right.asFile());
            }
        };
    }

    private MongodConfig createConfiguration(String rsName, int p)
            throws IOException {
        return ImmutableMongodConfig.builder()
                .version(Versions.withFeatures(() -> VERSION, Feature.NO_HTTP_INTERFACE_ARG))
                .net(new Net(InetAddress.getLoopbackAddress().getHostAddress(), p, false))
                .replication(newStorage(p, rsName))
                // enable journal
                .cmdOptions(ImmutableMongoCmdOptions.builder()
                        .useNoPrealloc(false)
                        .useNoJournal(false)
                        .useSmallFiles(false)
                        .build())
                .build();
    }

    private Storage newStorage(int port, String rs) throws IOException {
        File dbPath = new File(TMP_DIR.asFile(), "mongod-" + port);
        if (dbPath.exists()) {
            FileUtils.deleteDirectory(dbPath);
        }
        int oplogSize = rs != null ? 512 : 0;
        return new Storage(dbPath.getAbsolutePath(), rs, oplogSize);
    }
}
