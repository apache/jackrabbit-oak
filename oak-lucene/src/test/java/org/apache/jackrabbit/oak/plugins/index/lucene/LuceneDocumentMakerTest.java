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

package org.apache.jackrabbit.oak.plugins.index.lucene;

import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.index.lucene.util.IndexDefinitionBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.junit.Test;

import static java.util.Arrays.asList;
import static org.apache.jackrabbit.oak.InitialContent.INITIAL_CONTENT;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class LuceneDocumentMakerTest {
    private NodeState root = INITIAL_CONTENT;
    private IndexDefinitionBuilder builder = new IndexDefinitionBuilder();

    @Test
    public void excludeSingleProperty() throws Exception{
        builder.indexRule("nt:base")
                .property("foo")
                .propertyIndex()
                .analyzed()
                .valueExcludedPrefixes("/jobs");

        IndexDefinition defn = IndexDefinition.newBuilder(root, builder.build(), "/foo").build();
        LuceneDocumentMaker docMaker = new LuceneDocumentMaker(defn,
                defn.getApplicableIndexingRule("nt:base"), "/x");

        NodeBuilder test = EMPTY_NODE.builder();
        test.setProperty("foo", "bar");

        assertNotNull(docMaker.makeDocument(test.getNodeState()));

        test.setProperty("foo", "/jobs/a");
        assertNull(docMaker.makeDocument(test.getNodeState()));

        test.setProperty("foo", asList("/a", "/jobs/a"), Type.STRINGS);
        assertNotNull(docMaker.makeDocument(test.getNodeState()));

        test.setProperty("foo", asList("/jobs/a"), Type.STRINGS);
        assertNull(docMaker.makeDocument(test.getNodeState()));
    }

}