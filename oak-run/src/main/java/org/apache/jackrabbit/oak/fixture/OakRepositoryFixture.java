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
import javax.jcr.Repository;

import org.apache.jackrabbit.api.JackrabbitRepository;
import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.jcr.Jcr;

public class OakRepositoryFixture implements RepositoryFixture {

    public static RepositoryFixture getMemoryNS(long cacheSize) {
        return getMemory(OakFixture.OAK_MEMORY_NS, cacheSize);
    }

    private static RepositoryFixture getMemory(String name, long cacheSize) {
        return new OakRepositoryFixture(OakFixture.getMemory(name, cacheSize));
    }

    public static RepositoryFixture getMongo(String host, int port, String database,
                                             boolean dropDBAfterTest, long cacheSize) {
        return getMongo(OakFixture.OAK_MONGO, false, host, port, database, dropDBAfterTest, cacheSize, false, null, 0);
    }

    public static RepositoryFixture getMongoWithFDS(String host, int port, String database,
                                             boolean dropDBAfterTest, long cacheSize,
                                             final File base, int fdsCacheInMB) {
        return getMongo(OakFixture.OAK_MONGO_FDS, false, host, port, database,
                dropDBAfterTest, cacheSize, true, base, fdsCacheInMB);
    }

    public static RepositoryFixture getMongoMK(String host, int port, String database,
                                               boolean dropDBAfterTest, long cacheSize) {
        return getMongo(OakFixture.OAK_MONGO_MK, true, host, port, database, dropDBAfterTest, cacheSize, false, null, 0);
    }

    public static RepositoryFixture getMongoNS(String host, int port, String database,
                                               boolean dropDBAfterTest, long cacheSize) {
        return getMongo(OakFixture.OAK_MONGO_NS, false, host, port, database, dropDBAfterTest, cacheSize, false, null, 0);
    }

    private static RepositoryFixture getMongo(String name, boolean useMK,
                                              String host, int port, String database,
                                              boolean dropDBAfterTest, long cacheSize,
                                              final boolean useFileDataStore,
                                              final File base,
                                              final int fdsCacheInMB) {
        return new OakRepositoryFixture(OakFixture.getMongo(name, useMK, host, port, database, dropDBAfterTest,
                cacheSize, useFileDataStore, base, fdsCacheInMB));
    }

    public static RepositoryFixture getRDB(String jdbcuri, String jdbcuser, String jdbcpasswd, boolean dropDBAfterTest, long cacheSize) {
        return new OakRepositoryFixture(OakFixture.getRDB(OakFixture.OAK_RDB, jdbcuri, jdbcuser, jdbcpasswd, false, dropDBAfterTest,
                cacheSize));
    }

    public static RepositoryFixture getTar(File base, int maxFileSizeMB, int cacheSizeMB, boolean memoryMapping) {
        return new OakRepositoryFixture(OakFixture.getTar(OakFixture.OAK_TAR ,base, maxFileSizeMB, cacheSizeMB, memoryMapping, false));
    }

    public static RepositoryFixture getTarWithBlobStore(File base, int maxFileSizeMB, int cacheSizeMB, boolean memoryMapping) {
        return new OakRepositoryFixture(OakFixture.getTar(OakFixture.OAK_TAR_FDS,base, maxFileSizeMB, cacheSizeMB, memoryMapping, true));
    }


    private final OakFixture oakFixture;
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
        return setUpCluster(n,JcrCustomizer.DEFAULT);
    }

    public Repository[] setUpCluster(int n, JcrCustomizer customizer) throws Exception {
        Oak[] oaks = oakFixture.setUpCluster(n);
        cluster = new Repository[oaks.length];
        for (int i = 0; i < oaks.length; i++) {
            cluster[i] = customizer.customize(new Jcr(oaks[i])).createRepository();
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
}
