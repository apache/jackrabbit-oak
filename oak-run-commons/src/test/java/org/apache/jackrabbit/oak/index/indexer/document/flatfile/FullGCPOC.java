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
package org.apache.jackrabbit.oak.index.indexer.document.flatfile;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.util.concurrent.TimeUnit;

import org.apache.jackrabbit.oak.plugins.document.DocumentMK;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeStore;
import org.apache.jackrabbit.oak.plugins.document.LeaseCheckMode;
import org.apache.jackrabbit.oak.plugins.document.MongoUtils;
import org.apache.jackrabbit.oak.plugins.document.VersionGCStatsAccessor;
import org.apache.jackrabbit.oak.plugins.document.VersionGarbageCollector;
import org.apache.jackrabbit.oak.plugins.document.VersionGarbageCollector.VersionGCStats;
import org.apache.jackrabbit.oak.plugins.document.mongo.MongoDocumentNodeStoreBuilderBase;
import org.apache.jackrabbit.oak.plugins.document.mongo.MongoDocumentStore;
import org.apache.jackrabbit.oak.plugins.document.util.MongoConnection;
import org.slf4j.LoggerFactory;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;

public class FullGCPOC {

    public static void main(String[] args) throws Exception {
        final String beforeFilename = args[0];
        final String mongoDbName = args[1];
        final String afterFilename = args[2];
        final int gcRounds = Integer.parseInt(args[3]);

        System.setProperty("mongo.db", mongoDbName);
        assertEquals(mongoDbName, MongoUtils.DB);

        Logger sfft = (Logger) LoggerFactory.getLogger("org.apache.jackrabbit.oak.index.indexer.document.flatfile.SimpleFlatFileUtil");
        sfft.setLevel(Level.INFO);

        System.out.println("1) Initializing MongoDocumentStore with " + mongoDbName);
        MongoConnection connection = MongoUtils.getConnection();
        MongoDocumentNodeStoreBuilderBase<?> builder = new DocumentMK.Builder();
        MongoDocumentStore s = new MongoDocumentStore(connection.getMongoClient(),
                connection.getDatabase(), builder);

        System.out.println("2) Initializing DocumentNodeStore");
        DocumentNodeStore store = new DocumentMK.Builder().setDocumentStore(s)
                .setLeaseCheckMode(LeaseCheckMode.DISABLED).setDetailedGCEnabled(true)
                .setAsyncDelay(0).getNodeStore();
        try {
            File before = new File(beforeFilename);
            System.out.println("3) Exporting repository as flat file to "
                    + before.getCanonicalPath());
            SimpleFlatFileUtil.createFlatFileFor(store.getRoot(), before);

            VersionGarbageCollector gc = store.getVersionGarbageCollector();
            System.out.println("4) Doing " + gcRounds + " rounds of fullGc(24h)");
            VersionGCStats comboStats = new VersionGCStats();
            for (int i = gcRounds; i > 0; i--) {
                System.out.println("GC ROUND FOR i=" + i);
                VersionGCStats stats = gc.gc(24, TimeUnit.HOURS);
                System.out.println("GC stats : " + stats);
                VersionGCStatsAccessor.addRun(comboStats, stats);
            }
            System.out.println("Overall GC stats : " + comboStats);

            System.out.println("Overall GC stats highlights:");
            System.out.println(
                    " * ignoredDetailedGCDueToCheckPoint = " + VersionGCStatsAccessor
                            .getIgnoredDetailedGCDueToCheckPoint(comboStats));
            System.out.println(" * detailedGCDocsElapsed            = "
                    + VersionGCStatsAccessor.getDetailedGCDocsElapsed(comboStats));
            System.out.println(" * deleteDetailedGCDocsElapsed      = "
                    + VersionGCStatsAccessor.getDeleteDetailedGCDocsElapsed(comboStats));
            System.out.println(" * updatedDetailedGCDocsCount       = "
                    + VersionGCStatsAccessor.getUpdatedDetailedGCDocsCount(comboStats));

            File after = new File(afterFilename);
            System.out.println("5) Exporting repository as flat file to "
                    + after.getCanonicalPath());
            SimpleFlatFileUtil.createFlatFileFor(store.getRoot(), after);
        } finally {
            System.out.println("6) Disposing");
            store.dispose();
        }
        System.out.println("7) Success");
    }
}
