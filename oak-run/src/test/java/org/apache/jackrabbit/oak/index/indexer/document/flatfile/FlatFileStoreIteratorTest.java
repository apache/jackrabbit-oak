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

package org.apache.jackrabbit.oak.index.indexer.document.flatfile;

import java.util.Set;

import com.google.common.collect.ImmutableSet;
import org.apache.jackrabbit.oak.index.indexer.document.NodeStateEntry;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.junit.Test;

import static java.util.Arrays.asList;
import static org.apache.jackrabbit.oak.index.indexer.document.flatfile.TestUtils.createList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

public class FlatFileStoreIteratorTest {

    @Test
    public void simpleTraversal() {
        Set<String> preferred = ImmutableSet.of("jcr:content");
        CountingIterable<NodeStateEntry> citr = createList(preferred, asList("/a", "/a/jcr:content", "/a/jcr:content/metadata",
                "/a/d", "/e"));

        FlatFileStoreIterator fitr = new FlatFileStoreIterator(citr.iterator(), preferred.size());
        NodeStateEntry a = fitr.next();
        assertEquals("/a", a.getPath());

        NodeState ns1 = a.getNodeState().getChildNode("jcr:content");
        assertEquals("/a/jcr:content", ns1.getString("path"));
        assertEquals(1, fitr.getBufferSize());

        NodeState ns2 = ns1.getChildNode("metadata");
        assertEquals("/a/jcr:content/metadata", ns2.getString("path"));
        assertEquals(2, fitr.getBufferSize());

        NodeStateEntry nse1 = fitr.next();
        assertEquals("/a/jcr:content", nse1.getPath());

        NodeStateEntry nse2 = fitr.next();
        assertEquals("/a/jcr:content/metadata", nse2.getPath());

        NodeStateEntry nse3 = fitr.next();
        assertEquals("/a/d", nse3.getPath());
        assertEquals(0, nse3.getNodeState().getChildNodeCount(100));

        NodeStateEntry nse4 = fitr.next();
        assertEquals("/e", nse4.getPath());
        assertEquals(0, nse4.getNodeState().getChildNodeCount(100));

        assertFalse(fitr.hasNext());
    }

    @Test
    public void invalidOrderAccess() {
        Set<String> preferred = ImmutableSet.of("jcr:content");
        CountingIterable<NodeStateEntry> citr = createList(preferred, asList("/a", "/a/jcr:content", "/a/jcr:content/metadata",
                "/a/d", "/e"));

        FlatFileStoreIterator fitr = new FlatFileStoreIterator(citr.iterator(), preferred.size());
        NodeStateEntry a = fitr.next();
        assertEquals("/a", a.getPath());

        NodeState ns1 = a.getNodeState().getChildNode("jcr:content");

        NodeStateEntry nse1 = fitr.next();
        assertEquals("/a/jcr:content", nse1.getPath());
        assertEquals(1, nse1.getNodeState().getChildNodeCount(100));

        //Now move past /a/jcr:content
        NodeStateEntry nse2 = fitr.next();
        assertEquals("/a/jcr:content/metadata", nse2.getPath());

        try {
            //Now access from /a/jcr:content node should fail
            ns1.getChildNodeCount(100);
            fail("Access should have failed");
        } catch (IllegalStateException ignore) {

        }
    }

}