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
package org.apache.jackrabbit.oak.plugins.document.mongo;

import java.util.List;
import java.util.Set;

import com.mongodb.DB;

import org.apache.jackrabbit.oak.cache.CacheStats;
import org.apache.jackrabbit.oak.plugins.document.AbstractJournalTest;
import org.apache.jackrabbit.oak.plugins.document.DocumentMK;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeStore;
import org.apache.jackrabbit.oak.plugins.document.DocumentStore;
import org.apache.jackrabbit.oak.plugins.document.JournalGarbageCollector;
import org.apache.jackrabbit.oak.plugins.document.MongoConnectionFactory;
import org.apache.jackrabbit.oak.plugins.document.MongoUtils;
import org.apache.jackrabbit.oak.plugins.document.memory.MemoryDocumentStore;
import org.apache.jackrabbit.oak.plugins.document.util.MongoConnection;
import org.apache.jackrabbit.oak.spi.blob.MemoryBlobStore;
import org.apache.jackrabbit.oak.stats.Clock;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.assertEquals;

public class JournalIT extends AbstractJournalTest {

    private static final Logger LOG = LoggerFactory.getLogger(JournalIT.class);

    @Rule
    public MongoConnectionFactory connectionFactory = new MongoConnectionFactory();

    @BeforeClass
    public static void checkMongoDbAvailable() {
        Assume.assumeTrue(MongoUtils.isAvailable());
    }

    @Override
    public void clear() {
        super.clear();
        MongoConnection mongoConnection = connectionFactory.getConnection();
        MongoUtils.dropCollections(mongoConnection.getDB());
    }

    @Test
    public void cacheInvalidationTest() throws Exception {
        final DocumentNodeStore ns1 = createMK(1, 0).getNodeStore();
        final DocumentNodeStore ns2 = createMK(2, 0).getNodeStore();
        LOG.info("cache size 1: " + getCacheElementCount(ns1.getDocumentStore()));

        // invalidate cache under test first
        ns1.getDocumentStore().invalidateCache();

        {
            DocumentStore s = ns1.getDocumentStore();
            LOG.info("m.size=" + getCacheElementCount(s));
        }
        LOG.info("cache size 2: " + getCacheElementCount(ns1.getDocumentStore()));

        // first create child node in instance 1
        final List<String> paths = createRandomPaths(1, 5000000, 1000);
        int i=0;
        for(String path : paths) {
            if (i++%100==0) {
                LOG.info("at "+i);
            }
            getOrCreate(ns1, path, false);
        }
        final List<String> paths2 = createRandomPaths(20, 2345, 100);
        getOrCreate(ns1, paths2, false);
        ns1.runBackgroundOperations();
        for(String path : paths) {
            assertDocCache(ns1, true, path);
        }

        {
            DocumentStore s = ns1.getDocumentStore();
            LOG.info("m.size=" + getCacheElementCount(s));
        }

        LOG.info("cache size 2: " + getCacheElementCount(ns1.getDocumentStore()));
        long time = System.currentTimeMillis();
        for(int j=0; j<100; j++) {
            long now = System.currentTimeMillis();
            LOG.info("loop "+j+", "+(now-time)+"ms");
            time = now;
            final Set<String> electedPaths = choose(paths2, random.nextInt(30));
            {
                // choose a random few from above created paths and modify them
                final long t1 = System.currentTimeMillis();
                ns2.runBackgroundOperations(); // make sure ns2 has the latest from ns1
                final long t2 = System.currentTimeMillis();
                LOG.info("ns2 background took "+(t2-t1)+"ms");

                for(String electedPath : electedPaths) {
                    // modify /child in another instance 2
                    setProperty(ns2, electedPath, "p", "ns2"+System.currentTimeMillis(), false);
                }
                final long t3 = System.currentTimeMillis();
                LOG.info("setting props "+(t3-t2)+"ms");

                ns2.runBackgroundOperations();
                final long t4 = System.currentTimeMillis();
                LOG.info("ns2 background took2 "+(t4-t3)+"ms");
            }

            // that should not have changed the fact that we have it cached in 'ns1'
            for(String electedPath : electedPaths) {
                assertDocCache(ns1, true, electedPath);
            }

            // doing a backgroundOp now should trigger invalidation
            // which thx to the external modification will remove the entry from the cache:
            ns1.runBackgroundOperations();
            for(String electedPath : electedPaths) {
                assertDocCache(ns1, false, electedPath);
            }

            // when I access it again with 'ns1', then it gets cached again:
            for(String electedPath : electedPaths) {
                getOrCreate(ns1, electedPath, false);
                assertDocCache(ns1, true, electedPath);
            }
        }
    }

    @Test
    public void largeCleanupTest() throws Exception {
        // create more than DELETE_BATCH_SIZE of entries and clean them up
        // should make sure to loop in JournalGarbageCollector.gc such
        // that it would find issue described here:
        // https://issues.apache.org/jira/browse/OAK-2829?focusedCommentId=14585733&page=com.atlassian.jira.plugin.system.issuetabpanels:comment-tabpanel#comment-14585733

        doLargeCleanupTest(0, 100);
        doLargeCleanupTest(200, 1000);// using offset as to not make sure to always create new entries
        doLargeCleanupTest(2000, 10000);
        doLargeCleanupTest(20000, 30000); // using 'size' much larger than 30k will be tremendously slow due to ordered node
    }

    @Test
    public void simpleCacheInvalidationTest() throws Exception {
        final DocumentNodeStore ns1 = createMK(1, 0).getNodeStore();
        final DocumentNodeStore ns2 = createMK(2, 0).getNodeStore();

        // invalidate cache under test first
        ns1.getDocumentStore().invalidateCache();

        // first create child node in instance 1
        getOrCreate(ns1, "/child", true);
        assertDocCache(ns1, true, "/child");

        {
            // modify /child in another instance 2
            ns2.runBackgroundOperations(); // read latest changes from ns1
            setProperty(ns2, "/child", "p", "ns2"+System.currentTimeMillis(), true);
        }
        // that should not have changed the fact that we have it cached in 'ns'
        assertDocCache(ns1, true, "/child");

        // doing a backgroundOp now should trigger invalidation
        // which thx to the external modification will remove the entry from the cache:
        ns1.runBackgroundOperations();
        assertDocCache(ns1, false, "/child");

        // when I access it again with 'ns', then it gets cached again:
        getOrCreate(ns1, "/child", false);
        assertDocCache(ns1, true, "/child");
    }

    private void doLargeCleanupTest(int offset, int size) throws Exception {
        Clock clock = new Clock.Virtual();
        DocumentMK mk1 = createMK(0 /* clusterId: 0 => uses clusterNodes collection */, 0,
                new MemoryDocumentStore(), new MemoryBlobStore());
        DocumentNodeStore ns1 = mk1.getNodeStore();
        // make sure we're visible and marked as active
        renewClusterIdLease(ns1);
        JournalGarbageCollector gc = new JournalGarbageCollector(ns1, 0);
        clock.getTimeIncreasing();
        clock.getTimeIncreasing();
        gc.gc(); // cleanup everything that might still be there

        // create entries as parametrized:
        for(int i=offset; i<size+offset; i++) {
            mk1.commit("/", "+\"regular"+i+"\": {}", null, null);
            // always run background ops to 'flush' the change
            // into the journal:
            ns1.runBackgroundOperations();
        }
        Thread.sleep(100); // sleep 100millis
        assertEquals(size, gc.gc()); // should now be able to clean up everything
    }

    protected DocumentMK createMK(int clusterId, int asyncDelay) {
        DB db = connectionFactory.getConnection().getDB();
        builder = newDocumentMKBuilder();
        return register(builder.setMongoDB(db)
                .setClusterId(clusterId).setAsyncDelay(asyncDelay).setBundlingDisabled(true).open());
    }

    private static long getCacheElementCount(DocumentStore ds) {
        if (ds.getCacheStats() == null) {
            return -1;
        }

        long count = 0;
        for (CacheStats cacheStats : ds.getCacheStats()) {
            count += cacheStats.getElementCount();
        }

        return count;
    }

}
