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

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.apache.jackrabbit.oak.plugins.document.DocumentMK.Builder.DEFAULT_CHILDREN_CACHE_PERCENTAGE;
import static org.apache.jackrabbit.oak.plugins.document.DocumentMK.Builder.DEFAULT_DIFF_CACHE_PERCENTAGE;
import static org.apache.jackrabbit.oak.plugins.document.DocumentMK.Builder.DEFAULT_PREV_DOC_CACHE_PERCENTAGE;
import static org.junit.Assert.assertEquals;

public class DisableCacheTest {

    @Rule
    public DocumentMKBuilderProvider builderProvider = new DocumentMKBuilderProvider();

    @Rule
    public TemporaryFolder temp = new TemporaryFolder(new File("target"));

    @Test
    public void disableNodeCache() throws Exception {
        File cacheFolder = temp.newFolder();
        DocumentMK.Builder builder = builderProvider.newBuilder();
        builder.setPersistentCache(cacheFolder.getAbsolutePath());

        builder.memoryCacheDistribution(0,
                DEFAULT_PREV_DOC_CACHE_PERCENTAGE,
                DEFAULT_CHILDREN_CACHE_PERCENTAGE,
                DEFAULT_DIFF_CACHE_PERCENTAGE);
        DocumentNodeStore nodeStore = builder.getNodeStore();
        Cache<PathRev, DocumentNodeState> cache = nodeStore.getNodeCache();
        assertEquals(0, cache.size());
    }
}
