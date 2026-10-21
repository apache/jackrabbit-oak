/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.plugins.index.luceneNg;

import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class LuceneNgIndexStorageTest {

    @Test
    public void storagePathAppendsStorageNodeName() {
        assertEquals(
                "/oak:index/myIndex/" + LuceneNgIndexStorage.STORAGE_NODE_NAME,
                LuceneNgIndexStorage.storagePath("/oak:index/myIndex"));
    }

    @Test
    public void storageStateReadsChildNamedLikeStorageNode() {
        NodeBuilder def = EmptyNodeState.EMPTY_NODE.builder();
        assertFalse(LuceneNgIndexStorage.storageState(def.getNodeState()).exists());

        def.child(LuceneNgIndexStorage.STORAGE_NODE_NAME);
        assertTrue(LuceneNgIndexStorage.storageState(def.getNodeState()).exists());
    }

    @Test
    public void getOrCreateStorageBuilderSetsPrimaryTypeOnce() {
        NodeBuilder def = EmptyNodeState.EMPTY_NODE.builder();
        NodeBuilder s1 = LuceneNgIndexStorage.getOrCreateStorageBuilder(def);
        assertTrue(s1.getNodeState().exists());
        assertTrue(s1.hasProperty(JcrConstants.JCR_PRIMARYTYPE));

        NodeBuilder s2 = LuceneNgIndexStorage.getOrCreateStorageBuilder(def);
        assertEquals(s1.getNodeState(), s2.getNodeState());
    }
}
