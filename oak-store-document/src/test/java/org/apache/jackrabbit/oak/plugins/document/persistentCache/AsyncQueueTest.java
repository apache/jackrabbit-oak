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
package org.apache.jackrabbit.oak.plugins.document.persistentCache;

import org.apache.commons.io.FileUtils;
import org.apache.jackrabbit.guava.common.cache.RemovalCause;
import org.apache.jackrabbit.oak.cache.CacheLIRS;
import org.apache.jackrabbit.oak.commons.collections.CollectionUtils;
import org.apache.jackrabbit.oak.plugins.document.DocumentMKBuilderProvider;
import org.apache.jackrabbit.oak.plugins.document.Path;
import org.apache.jackrabbit.oak.plugins.document.PathRev;
import org.apache.jackrabbit.oak.plugins.document.Revision;
import org.apache.jackrabbit.oak.plugins.document.RevisionVector;
import org.apache.jackrabbit.oak.plugins.document.persistentCache.async.CacheWriteQueue;
import org.apache.jackrabbit.oak.plugins.document.util.StringValue;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static org.junit.Assert.assertEquals;

public class AsyncQueueTest {

    @Rule
    public DocumentMKBuilderProvider builderProvider = new DocumentMKBuilderProvider();

    private static final StringValue VAL = new StringValue("xyz");

    private PersistentCache pCache;

    private List<PathRev> putActions;

    private List<PathRev> invalidateActions;

    private NodeCache<PathRev, StringValue> nodeCache;

    private int id;

    @Before
    public void setup() throws IOException {
        FileUtils.deleteDirectory(new File("target/cacheTest"));
        pCache = new PersistentCache("target/cacheTest");
        final AtomicReference<NodeCache<PathRev, StringValue>> nodeCacheRef = new AtomicReference<NodeCache<PathRev, StringValue>>();
        CacheLIRS<PathRev, StringValue> cache = new CacheLIRS.Builder<PathRev, StringValue>().maximumSize(1).evictionCallback(new CacheLIRS.EvictionCallback<PathRev, StringValue>() {
            @Override
            public void evicted(@NotNull PathRev key, @Nullable StringValue value, @NotNull RemovalCause cause) {
                if (nodeCacheRef.get() != null) {
                    nodeCacheRef.get().evicted(key, value, cause);
                }
            }
        }).build();
        nodeCache = (NodeCache<PathRev, StringValue>) pCache.wrap(builderProvider.newBuilder().getNodeStore(),
                null, cache,  CacheType.NODE);
        nodeCacheRef.set(nodeCache);

        CacheWriteQueueWrapper writeQueue = new CacheWriteQueueWrapper(nodeCache.writeQueue);
        nodeCache.writeQueue = writeQueue;

        this.putActions = writeQueue.putActions;
        this.invalidateActions = writeQueue.invalidateActions;
        this.id = 0;
    }

    @After
    public void teardown() {
        if (pCache != null) {
            pCache.close();
        }
    }
    
    @Test
    public void unusedItemsShouldntBePersisted() {
        PathRev k = generatePathRev();
        nodeCache.put(k, VAL);
        flush();
        assertEquals(emptyList(), putActions);
    }

    @Test
    public void readItemsShouldntBePersistedAgain() {
        PathRev k = generatePathRev();
        nodeCache.put(k, VAL);
        nodeCache.getIfPresent(k);
        flush();
        assertEquals(asList(k), putActions);

        putActions.clear();
        nodeCache.getIfPresent(k); // k should be loaded from persisted cache
        flush();
        assertEquals(emptyList(), putActions); // k is not persisted again
    }

    @Test
    public void usedItemsShouldBePersisted() {
        PathRev k = generatePathRev();
        nodeCache.put(k, VAL);
        nodeCache.getIfPresent(k);
        flush();
        assertEquals(asList(k), putActions);
    }

    private PathRev generatePathRev() {
        return new PathRev(Path.fromString("/" + id++), new RevisionVector(new Revision(0, 0, 0)));
    }

    private void flush() {
        for (int i = 0; i < 1024; i++) {
            nodeCache.put(generatePathRev(), VAL); // cause eviction of k
        }
    }

    private static class CacheWriteQueueWrapper extends CacheWriteQueue<PathRev, StringValue> {

        private final CacheWriteQueue<PathRev, StringValue>  wrapped;

        private final List<PathRev> putActions = new ArrayList<>();

        private final List<PathRev> invalidateActions = new ArrayList<>();

        public CacheWriteQueueWrapper(CacheWriteQueue<PathRev, StringValue>  wrapped) {
            super(null, null, null);
            this.wrapped = wrapped;
        }

        @Override
        public boolean addPut(PathRev key, StringValue value) {
            putActions.add(key);
            return wrapped.addPut(key, value);
        }

        public boolean addInvalidate(Iterable<PathRev> keys) {
            invalidateActions.addAll(CollectionUtils.toList(keys));
            return wrapped.addInvalidate(keys);
        }
    }

}
