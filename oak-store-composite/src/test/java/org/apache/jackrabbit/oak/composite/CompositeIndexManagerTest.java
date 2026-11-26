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
package org.apache.jackrabbit.oak.composite;

import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.ASYNC_PROPERTY_NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_DEFINITIONS_NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_DEFINITIONS_NODE_TYPE;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.TYPE_PROPERTY_NAME;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.List;

import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.index.IndexDefinitionAudit;
import org.apache.jackrabbit.oak.plugins.index.IndexManagerMBeanImpl;
import org.apache.jackrabbit.oak.plugins.index.property.PropertyIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.mount.MountInfoProvider;
import org.apache.jackrabbit.oak.spi.mount.Mounts;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Deleting an index with the Index Manager when part of its index data is in a read-only mount.
 */
public class CompositeIndexManagerTest {

    private static final String MOUNT_DATA = ":oak:mount-libs-index-data";

    private String auditProperty;

    @Before
    public void enableAudit() {
        auditProperty = System.getProperty(IndexDefinitionAudit.ENABLED_PROPERTY);
        System.setProperty(IndexDefinitionAudit.ENABLED_PROPERTY, "true");
    }

    @After
    public void restoreAudit() {
        if (auditProperty == null) {
            System.clearProperty(IndexDefinitionAudit.ENABLED_PROPERTY);
        } else {
            System.setProperty(IndexDefinitionAudit.ENABLED_PROPERTY, auditProperty);
        }
    }

    @Test
    public void deleteIndexWithIndexDataInReadOnlyMount() throws Exception {
        MemoryNodeStore global = new MemoryNodeStore();
        MemoryNodeStore libs = new MemoryNodeStore();
        NodeBuilder builder = libs.getRoot().builder();
        builder.child("libs");
        builder.child(INDEX_DEFINITIONS_NAME).child("fooIndex").child(MOUNT_DATA).child("n0").setProperty("x", 1L);
        libs.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        builder = global.getRoot().builder();
        NodeBuilder index = builder.child(INDEX_DEFINITIONS_NAME).child("fooIndex");
        index.setProperty(JcrConstants.JCR_PRIMARYTYPE, INDEX_DEFINITIONS_NODE_TYPE, Type.NAME);
        index.setProperty(TYPE_PROPERTY_NAME, "disabled");
        index.setProperty("originalType", "lucene");
        index.setProperty(ASYNC_PROPERTY_NAME, List.of("async"), Type.STRINGS);
        for (int i = 0; i < 3; i++) {
            index.child(":data").child("n" + i).setProperty("x", (long) i);
        }
        global.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        MountInfoProvider mip = Mounts.newBuilder()
                .mount("libs", true, List.of("/oak:index/*$"), List.of("/libs"))
                .build();
        NodeStore store = new CompositeNodeStore.Builder(mip, global).addMount("libs", libs).build();
        NodeState definition = store.getRoot().getChildNode(INDEX_DEFINITIONS_NAME).getChildNode("fooIndex");
        assertTrue(definition.hasChildNode(MOUNT_DATA));
        assertTrue(definition.hasChildNode(":data"));

        IndexManagerMBeanImpl mbean = new IndexManagerMBeanImpl(store, new PropertyIndexEditorProvider(), null);
        mbean.deleteIndex("fooIndex", "alice", "unused", false);

        assertFalse(store.getRoot().getChildNode(INDEX_DEFINITIONS_NAME).hasChildNode("fooIndex"));
        assertFalse(global.getRoot().getChildNode(INDEX_DEFINITIONS_NAME).hasChildNode("fooIndex"));
        // the read-only mount is not changed
        assertTrue(libs.getRoot().getChildNode(INDEX_DEFINITIONS_NAME).getChildNode("fooIndex").hasChildNode(MOUNT_DATA));
    }
}
