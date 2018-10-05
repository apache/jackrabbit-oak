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
package org.apache.jackrabbit.oak.plugins.document;

import java.io.File;

import com.google.common.cache.Cache;

import org.apache.commons.io.FileUtils;
import org.junit.Rule;
import org.junit.Test;

import static org.junit.Assert.assertNull;

public class AsyncCacheTest {

    @Rule
    public DocumentMKBuilderProvider builderProvider = new DocumentMKBuilderProvider();

    @Test
    public void invalidateWhileInQueue() throws Exception {
        FileUtils.deleteDirectory(new File("target/cacheTest"));
        DocumentMK.Builder builder = builderProvider.newBuilder();
        builder.setPersistentCache("target/cacheTest");
        DocumentNodeStore nodeStore = builder.getNodeStore();
        Cache<PathRev, DocumentNodeState.Children> cache = builder.buildChildrenCache(nodeStore);
        DocumentNodeState.Children c = new DocumentNodeState.Children();
        for (int i = 0; i < 100; i++) {
            c.children.add("node-" + i);
        }
        PathRev key = null;
        for (int i = 0; i < 1000; i++) {
            key = new PathRev("/foo/bar", new RevisionVector(new Revision(i, 0, 1)));
            cache.put(key, c);
        }
        cache.invalidate(key);
        // give the write queue some time to write back entries
        Thread.sleep(200);
        assertNull(cache.getIfPresent(key));
        builder.getPersistentCache().close();
    }
}
