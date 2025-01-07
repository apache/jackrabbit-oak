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

import org.apache.jackrabbit.oak.exporter.NodeStateExportCommand;
import org.apache.jackrabbit.oak.index.IndexCommand;
import org.apache.jackrabbit.oak.index.merge.IndexDiffCommand;
import org.apache.jackrabbit.oak.index.merge.IndexStoreCommand;
import org.apache.jackrabbit.oak.run.commons.Command;
import org.apache.jackrabbit.oak.run.commons.Modes;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public final class AvailableModes {
    // list of available Modes for the tool
    public static final Modes MODES = new Modes(createMap());

    private static Map<String, Command> createMap() {
        Map<String, Command> builder = new HashMap<>();
        builder.put("backup", new BackupCommand());
        builder.put("check", new CheckCommand());
        builder.put("checkpoints", new CheckpointsCommand());
        builder.put("clusternodes", new ClusterNodesCommand());
        builder.put("compact", new CompactCommand());
        builder.put("composite-prepare", new CompositePrepareCommand());
        builder.put("console", new ConsoleCommand());
        builder.put(DataStoreCommand.NAME, new DataStoreCommand());
        builder.put(DataStoreCopyCommand.NAME, new DataStoreCopyCommand());
        builder.put("datastorecacheupgrade", new DataStoreCacheUpgradeCommand());
        builder.put("datastorecheck", new DataStoreCheckCommand());
        builder.put("debug", new DebugCommand());
        builder.put(DocumentStoreCheckCommand.NAME, new DocumentStoreCheckCommand());
        builder.put("explore", new ExploreCommand());
        builder.put(NodeStateExportCommand.NAME, new NodeStateExportCommand());
        builder.put(FlatFileCommand.NAME, new FlatFileCommand());
        builder.put(FrozenNodeRefsByScanningCommand.NAME, new FrozenNodeRefsByScanningCommand());
        builder.put(FrozenNodeRefsUsingIndexCommand.NAME, new FrozenNodeRefsUsingIndexCommand());
        builder.put("garbage", new GarbageCommand());
        builder.put("help", new HelpCommand());
        builder.put("history", new HistoryCommand());
        builder.put("index-diff", new IndexDiffCommand());
        builder.put("index-merge", new IndexMergeCommand());
        builder.put(IndexStoreCommand.INDEX_STORE, new IndexStoreCommand());
        builder.put(IndexCommand.NAME, new IndexCommand());
        builder.put(IOTraceCommand.NAME, new IOTraceCommand());
        builder.put(JsonIndexCommand.INDEX, new JsonIndexCommand());
        builder.put(PersistentCacheCommand.PERSISTENTCACHE, new PersistentCacheCommand());
        builder.put("rdbddldump", new RDBDDLDumpCommand());
        builder.put("recovery", new RecoveryCommand());
        builder.put("recover-journal", new RecoverJournalCommand());
        builder.put("revisions", new RevisionsCommand());
        builder.put("repair", new RepairCommand());
        builder.put("resetclusterid", new ResetClusterIdCommand());
        builder.put("restore", new RestoreCommand());
        builder.put("tarmkdiff", new FileStoreDiffCommand());
        builder.put(ThreadDumpCommand.THREADDUMP, new ThreadDumpCommand());
        builder.put("tika", new TikaCommand());
        builder.put("unlockupgrade", new UnlockUpgradeCommand());
        builder.put("upgrade", new UpgradeCommand());
        builder.put("search-nodes", new SearchNodesCommand());
        builder.put("segment-copy", new SegmentCopyCommand());
        builder.put("server", new ServerCommand());
        builder.put("purge-index-versions", new LucenePurgeOldIndexVersionCommand());
        builder.put("create-test-garbage", new CreateGarbageCommand());

        return Collections.unmodifiableMap(builder);
    }
}
