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
import de.flapdoodle.embed.mongo.config.IMongodConfig;
import de.flapdoodle.embed.mongo.config.MongoCmdOptionsBuilder;
import de.flapdoodle.embed.mongo.config.MongodConfigBuilder;
import de.flapdoodle.embed.mongo.config.Net;
import de.flapdoodle.embed.mongo.config.Storage;
import de.flapdoodle.embed.mongo.distribution.Versions;
import de.flapdoodle.embed.process.io.directories.FixedPath;
import de.flapdoodle.embed.process.io.directories.IDirectory;
import de.flapdoodle.embed.process.runtime.IStopable;

import static de.flapdoodle.embed.process.io.directories.Directories.join;

/**
 * Helper class for starting/stopping a mongod process.
 */
public class MongodProcess {

    private static final String VERSION = "3.4.10";

    private static final IDirectory TMP_DIR = join(new FixedPath("target"), new FixedPath("tmp"));

    private IStopable process;

    private final MongodStarter starter;

    private final IMongodConfig config;

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

    private IMongodConfig createConfiguration(String rsName, int p)
            throws IOException {
        return new MongodConfigBuilder()
                .version(Versions.withFeatures(() -> VERSION))
                .net(new Net(InetAddress.getLoopbackAddress().getHostAddress(), p, false))
                .replication(newStorage(p, rsName))
                // enable journal
                .cmdOptions(new MongoCmdOptionsBuilder().useNoJournal(false).build())
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
