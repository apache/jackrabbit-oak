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
package org.apache.jackrabbit.oak.fixture;

import java.io.File;
import java.util.Collections;

import javax.jcr.Repository;

import org.apache.jackrabbit.api.JackrabbitRepository;
import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;

public class OakRepositoryFixture implements RepositoryFixture {

    public static RepositoryFixture getMemoryNS(long cacheSize) {
        return getMemory(OakFixture.OAK_MEMORY_NS, cacheSize);
    }

    private static RepositoryFixture getMemory(String name, long cacheSize) {
        return new OakRepositoryFixture(OakFixture.getMemory(name, cacheSize));
    }

    public static RepositoryFixture getMongo(String host, int port, String database,
                                             boolean dropDBAfterTest, long cacheSize) {
        return getMongo(OakFixture.OAK_MONGO, host, port, database, dropDBAfterTest, cacheSize, false, null, 0);
    }

    public static RepositoryFixture getMongo(String uri,
                                             boolean dropDBAfterTest, long cacheSize) {
        return getMongoNS(uri, dropDBAfterTest, cacheSize);
    }

    public static RepositoryFixture getMongoWithDS(String host, int port, String database,
                                             boolean dropDBAfterTest, long cacheSize,
                                             final File base, int fdsCacheInMB) {
        return getMongo(OakFixture.OAK_MONGO_DS, host, port, database,
                dropDBAfterTest, cacheSize, true, base, fdsCacheInMB);
    }

    public static RepositoryFixture getMongoWithDS(String uri,
                                                    boolean dropDBAfterTest, long cacheSize,
                                                    final File base, int fdsCacheInMB) {
        return new OakRepositoryFixture(OakFixture.getMongo(OakFixture.OAK_MONGO_DS, uri, dropDBAfterTest,
                cacheSize, true, base, fdsCacheInMB));
    }

    public static RepositoryFixture getMongoNS(String host, int port, String database,
                                               boolean dropDBAfterTest, long cacheSize) {
        return getMongo(OakFixture.OAK_MONGO_NS, host, port, database, dropDBAfterTest, cacheSize, false, null, 0);
    }

    public static RepositoryFixture getMongoNS(String uri,
                                               boolean dropDBAfterTest, long cacheSize) {
        return new OakRepositoryFixture(OakFixture.getMongo(uri, dropDBAfterTest, cacheSize));
    }

    private static RepositoryFixture getMongo(String name,
                                              String host, int port, String database,
                                              boolean dropDBAfterTest, long cacheSize,
                                              final boolean useFileDataStore,
                                              final File base,
                                              final int fdsCacheInMB) {
        return new OakRepositoryFixture(OakFixture.getMongo(name, host, port, database, dropDBAfterTest,
                cacheSize, useFileDataStore, base, fdsCacheInMB));
    }

    public static RepositoryFixture getRDB(String jdbcuri, String jdbcuser, String jdbcpasswd,
        String jdbctableprefix, boolean dropDBAfterTest, long cacheSize, int vgcMaxAge) {
        return new OakRepositoryFixture(OakFixture
            .getRDB(OakFixture.OAK_RDB, jdbcuri, jdbcuser, jdbcpasswd, jdbctableprefix,
                dropDBAfterTest, cacheSize, vgcMaxAge));
    }

    public static RepositoryFixture getRDBWithDS(String jdbcuri, String jdbcuser, String jdbcpasswd,
        String jdbctableprefix, boolean dropDBAfterTest, long cacheSize, File base,
        int fdsCacheInMB, int vgcMaxAge) {
        return new OakRepositoryFixture(OakFixture
            .getRDB(OakFixture.OAK_RDB_DS, jdbcuri, jdbcuser, jdbcpasswd, jdbctableprefix,
                dropDBAfterTest, cacheSize, true, base, fdsCacheInMB, vgcMaxAge));
    }

    public static RepositoryFixture getSegmentTar(File base, int maxFileSizeMB, int cacheSizeMB,
            boolean memoryMapping) {
        return new OakRepositoryFixture(
                OakFixture.getVanillaSegmentTar(base, maxFileSizeMB, cacheSizeMB, memoryMapping));
    }

    public static RepositoryFixture getSegmentTarWithDataStore(File base, int maxFileSizeMB, int cacheSizeMB,
            boolean memoryMapping, int dsCacheInMB) {
        return new OakRepositoryFixture(
                OakFixture.getSegmentTarWithDataStore(base, maxFileSizeMB, cacheSizeMB, memoryMapping, dsCacheInMB));
    }
    
    public static RepositoryFixture getSegmentTarWithColdStandby(File base, int maxFileSizeMB, int cacheSizeMB,
            boolean memoryMapping, boolean useBlobStore, int dsCacheInMB, int syncInterval, boolean shareBlobStore, 
            boolean secure, boolean oneShotRun) {
        return new OakRepositoryFixture(OakFixture.getSegmentTarWithColdStandby(base, maxFileSizeMB, cacheSizeMB,
                memoryMapping, useBlobStore, dsCacheInMB, syncInterval, shareBlobStore, secure, oneShotRun));
    }

    public static RepositoryFixture getCompositeStore(File base, int maxFileSizeMB, int cacheSizeMB,
                                                      final boolean memoryMapping, int mounts, int pathsPerMount) {
        return new OakRepositoryFixture(OakFixture.getCompositeStore(OakFixture.OAK_COMPOSITE_STORE,
                base, maxFileSizeMB, cacheSizeMB, memoryMapping, mounts, pathsPerMount));
    }

    public static RepositoryFixture getCompositeMemoryStore(int mounts, int pathsPerMount) {
        return new OakRepositoryFixture(OakFixture.getCompositeMemoryStore(OakFixture.OAK_COMPOSITE_MEMORY_STORE, mounts, pathsPerMount));
    }

    private final OakFixture oakFixture;
    private StatisticsProvider statisticsProvider = StatisticsProvider.NOOP;
    private Repository[] cluster;

    protected OakRepositoryFixture(OakFixture oakFixture) {
        this.oakFixture = oakFixture;
    }

    @Override
    public boolean isAvailable(int n) {
        return true;
    }

    @Override
    public final Repository[] setUpCluster(int n) throws Exception {
        return setUpCluster(n, JcrCreator.DEFAULT);
    }

    public Repository[] setUpCluster(int n, JcrCreator customizer) throws Exception {
        Oak[] oaks = oakFixture.setUpCluster(n, statisticsProvider);
        cluster = new Repository[oaks.length];
        for (int i = 0; i < oaks.length; i++) {
            configureStatsProvider(oaks[i]);
            cluster[i] = customizer.customize(oaks[i]).createRepository();
        }
        return cluster;
    }

    @Override
    public void syncRepositoryCluster(Repository... nodes) {
        throw new UnsupportedOperationException("TODO");
    }

    @Override
    public void tearDownCluster() {
        if (cluster != null) {
            for (Repository repo : cluster) {
                if (repo instanceof JackrabbitRepository) {
                    ((JackrabbitRepository) repo).shutdown();
                }
            }
        }
        oakFixture.tearDownCluster();
    }

    @Override
    public String toString() {
        return oakFixture.toString();
    }

    public OakFixture getOakFixture() {
        return oakFixture;
    }

    public void setStatisticsProvider(StatisticsProvider statisticsProvider) {
        this.statisticsProvider = statisticsProvider;
    }

    private void configureStatsProvider(Oak oak) {
        oak.getWhiteboard().register(StatisticsProvider.class, statisticsProvider, Collections.emptyMap());
    }
}