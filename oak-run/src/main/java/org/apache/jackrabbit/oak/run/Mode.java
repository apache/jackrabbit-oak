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

enum Mode {

    BACKUP("backup", new BackupCommand()),
    RESTORE("restore", new RestoreCommand()),
    BENCHMARK("benchmark", new BenchmarkCommand()),
    CONSOLE("console", new ConsoleCommand()),
    DEBUG("debug", new DebugCommand()),
    GRAPH("graph", new GraphCommand()),
    HISTORY("history", new HistoryCommand()),
    CHECK("check", new CheckCommand()),
    COMPACT("compact", new CompactCommand()),
    SERVER("server", new ServerCommand()),
    UPGRADE("upgrade", new UpgradeCommand()),
    UNLOCKUPGRADE("unlockUpgrade", new UnlockUpgradeCommand()),
    SCALABILITY("scalability", new ScalabilityCommand()),
    EXPLORE("explore", new ExploreCommand()),
    CHECKPOINTS("checkpoints", new CheckpointsCommand()),
    RECOVERY("recovery", new RecoveryCommand()),
    REPAIR("repair", new RepairCommand()),
    TIKA("tika", new TikaCommand()),
    GARBAGE("garbage", new GarbageCommand()),
    TARMKDIFF("tarmkdiff", new FileStoreDiffCommand()),
    DATASTORECHECK("datastorecheck", new DataStoreCheckCommand()),
    RESETCLUSTERID("resetclusterid", new ResetClusterIdCommand()),
    PERSISTENTCACHE("persistentcache", new PersistentCacheCommand()),
    THREADDUMP("threaddump", new ThreadDumpCommand()),
    DATASTORECACHEUPGRADE("datastorecacheupgrade", new DataStoreCacheUpgradeCommand()),
    INDEX("index", new IndexCommand()),
    HELP("help", new HelpCommand());

    private final String name;

    private final Command command;

    Mode(String name, Command command) {
        this.name = name;
        this.command = command;
    }

    @Override
    public String toString() {
        return name;
    }

    public void execute(String... args) throws Exception {
        command.execute(args);
    }

}