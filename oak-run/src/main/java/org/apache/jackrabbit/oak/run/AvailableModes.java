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

package org.apache.jackrabbit.oak.run;

import com.google.common.collect.ImmutableMap;
import org.apache.jackrabbit.oak.exporter.NodeStateExportCommand;
import org.apache.jackrabbit.oak.index.IndexCommand;
import org.apache.jackrabbit.oak.run.commons.Command;
import org.apache.jackrabbit.oak.run.commons.Modes;

public final class AvailableModes {
    // list of available Modes for the tool
    public static final Modes MODES = new Modes(
        ImmutableMap.<String, Command>builder()
            .put("backup", new BackupCommand())
            .put("checkpoints", new CheckpointsCommand())
            .put("check", new CheckCommand())
            .put("datastorecacheupgrade", new DataStoreCacheUpgradeCommand())
            .put("compact", new CompactCommand())
            .put("composite-prepare", new CompositePrepareCommand())
            .put("console", new ConsoleCommand())
            .put("datastorecheck", new DataStoreCheckCommand())
            .put("debug", new DebugCommand())
            .put("explore", new ExploreCommand())
            .put("garbage", new GarbageCommand())
            .put("help", new HelpCommand())
            .put("history", new HistoryCommand())
            .put(JsonIndexCommand.INDEX, new JsonIndexCommand())
            .put(PersistentCacheCommand.PERSISTENTCACHE, new PersistentCacheCommand())
            .put("revisions", new RevisionsCommand())
            .put("recovery", new RecoveryCommand())
            .put("repair", new RepairCommand())
            .put("resetclusterid", new ResetClusterIdCommand())
            .put("restore", new RestoreCommand())
            .put("tarmkdiff", new FileStoreDiffCommand())
            .put(ThreadDumpCommand.THREADDUMP, new ThreadDumpCommand())
            .put("tika", new TikaCommand())
            .put("upgrade", new UpgradeCommand())
            .put("unlockupgrade", new UnlockUpgradeCommand())
            .put(IndexCommand.NAME, new IndexCommand())
            .put(NodeStateExportCommand.NAME, new NodeStateExportCommand())
            .put("server", new ServerCommand())
            .build());
}
