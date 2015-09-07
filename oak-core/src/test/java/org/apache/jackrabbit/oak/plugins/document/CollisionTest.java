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

import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Rule;
import org.junit.Test;

import static org.apache.jackrabbit.oak.plugins.document.Collection.NODES;
import static org.apache.jackrabbit.oak.plugins.document.NodeDocument.COLLISIONS;
import static org.apache.jackrabbit.oak.plugins.document.util.Utils.getIdFromPath;
import static org.junit.Assert.assertEquals;

public class CollisionTest {

    @Rule
    public DocumentMKBuilderProvider builderProvider = new DocumentMKBuilderProvider();
    
    private static final AtomicInteger COUNTER = new AtomicInteger();

    // OAK-2342
    @Test
    public void purge() throws Exception {
        DocumentMK mk1 = builderProvider.newBuilder().setClusterId(1).open();
        DocumentNodeStore ns1 = mk1.getNodeStore();
        DocumentStore store = ns1.getDocumentStore();

        DocumentMK mk2 = builderProvider.newBuilder().setClusterId(2)
                .setDocumentStore(store).open();
        DocumentNodeStore ns2 = mk2.getNodeStore();

        createCollision(mk1);
        createCollision(mk2);

        String id = getIdFromPath("/");
        assertEquals(2, store.find(NODES, id).getLocalMap(COLLISIONS).size());

        // restart node store
        ns1.dispose();
        mk1 = builderProvider.newBuilder().setClusterId(1)
                .setDocumentStore(store).open();
        ns1 = mk1.getNodeStore();

        // must purge collision for clusterId 1
        assertEquals(1, store.find(NODES, id).getLocalMap(COLLISIONS).size());
        ns1.dispose();

        // restart other node store
        ns2.dispose();
        mk2 = builderProvider.newBuilder().setClusterId(2)
                .setDocumentStore(store).open();
        ns2 = mk2.getNodeStore();

        // must purge collision for clusterId 2
        assertEquals(0, store.find(NODES, id).getLocalMap(COLLISIONS).size());
        ns2.dispose();
    }

    private void createCollision(DocumentMK mk) throws Exception {
        String nodeName = "test-" + COUNTER.getAndIncrement();
        // create branch
        String b = mk.branch(null);
        mk.commit("/", "+\"" + nodeName + "\":{\"p\":\"a\"}", b, null);

        // commit a change resulting in a collision on branch
        mk.commit("/", "+\"" + nodeName + "\":{\"p\":\"b\"}", null, null);
    }

}
