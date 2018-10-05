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
package org.apache.jackrabbit.oak.benchmark;

import com.google.common.cache.Cache;
import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.fixture.OakFixture;
import org.apache.jackrabbit.oak.fixture.OakRepositoryFixture;
import org.apache.jackrabbit.oak.fixture.RepositoryFixture;
import org.apache.jackrabbit.oak.jcr.Jcr;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeState;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeStore;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeStoreBuilder;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeStoreHelper;
import org.apache.jackrabbit.oak.plugins.document.PathRev;
import org.apache.jackrabbit.oak.plugins.document.Revision;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;

import javax.jcr.Repository;
import java.util.concurrent.atomic.AtomicLong;

public class PersistentCacheTest extends AbstractTest {

    private static final int ITEMS_TO_ADD = Integer.getInteger("items", 10000);

    private static final String CACHE_OPTIONS = System.getProperty("cacheOptions", "size=100,+compact,-async");

    private final StatisticsProvider statsProvider;

    private Cache<PathRev, DocumentNodeState> nodesCache;

    private DocumentNodeStore dns;

    private AtomicLong timestamp = new AtomicLong(1000);

    public PersistentCacheTest(StatisticsProvider statsProvider) {
        this.statsProvider = statsProvider;
    }

    @Override
    protected Repository[] createRepository(RepositoryFixture fixture) throws Exception {
        System.setProperty("PersistentCacheStats.rejectedPut", "true");
        if (fixture instanceof OakRepositoryFixture) {
            OakFixture oakFixture = ((OakRepositoryFixture) fixture).getOakFixture();
            if (oakFixture instanceof OakFixture.MongoFixture) {
                OakFixture.MongoFixture mongoFixture = (OakFixture.MongoFixture) oakFixture;
                DocumentNodeStoreBuilder<?> builder = mongoFixture.getBuilder(1);
                builder.setStatisticsProvider(statsProvider);
                builder.setPersistentCache("target/persistentCache,time," + CACHE_OPTIONS);
                dns = builder.build();
                nodesCache = DocumentNodeStoreHelper.getNodesCache(dns);
                Oak oak = new Oak(dns);
                return new Repository[] { new Jcr(oak).createRepository() };
            }
        }
        throw new IllegalArgumentException("Fixture " + fixture + " not supported for this benchmark.");
    }

    @Override
    protected void runTest() throws Exception {
        for (int i = 0; i < ITEMS_TO_ADD; i++) {
            PathRev key = PathRev.fromString("/" + timestamp.getAndIncrement() + "@" + new Revision(timestamp.getAndIncrement(), 0, 0));
            nodesCache.put(key, dns.getRoot());
            nodesCache.getIfPresent(key); // read, so the entry is marked as used
        }
    }
}