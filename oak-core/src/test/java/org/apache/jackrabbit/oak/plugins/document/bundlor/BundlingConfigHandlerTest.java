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

package org.apache.jackrabbit.oak.plugins.document.bundlor;


import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.junit.Test;

import static com.google.common.util.concurrent.MoreExecutors.sameThreadExecutor;
import static java.util.Collections.singletonList;
import static org.apache.jackrabbit.oak.plugins.document.bundlor.BundlingConfigHandler.BUNDLOR;
import static org.apache.jackrabbit.oak.plugins.document.bundlor.BundlingConfigHandler.DOCUMENT_NODE_STORE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class BundlingConfigHandlerTest {
    private BundlingConfigHandler configHandler = new BundlingConfigHandler();
    private MemoryNodeStore nodeStore = new MemoryNodeStore();

    @Test
    public void defaultSetup() throws Exception{
        assertNotNull(configHandler.getRegistry());
        assertNotNull(configHandler.newBundlingHandler());

        //Close should work without init also
        configHandler.close();
    }

    @Test
    public void detectRegistryUpdate() throws Exception{
        configHandler.initialize(nodeStore, sameThreadExecutor());
        addBundlorConfigForAsset();

        BundledTypesRegistry registry = configHandler.getRegistry();
        assertEquals(1, registry.getBundlors().size());
        DocumentBundlor assetBundlor = registry.getBundlors().get("app:Asset");
        assertNotNull(assetBundlor);
    }

    private void addBundlorConfigForAsset() throws CommitFailedException {
        NodeBuilder builder = nodeStore.getRoot().builder();
        NodeBuilder bundlor = builder.child("jcr:system").child(DOCUMENT_NODE_STORE).child(BUNDLOR);
        bundlor.child("app:Asset").setProperty(DocumentBundlor.PROP_PATTERN,
                singletonList("metadata"), Type.STRINGS);
        nodeStore.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
    }

}